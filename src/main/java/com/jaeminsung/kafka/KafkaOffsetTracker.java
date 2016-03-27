package com.jaeminsung.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.jaeminsung.domain.OffsetInfo;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

/**
 * KafkaOffsetTracker connects to the given Kafka brokers, to obtain 
 * the offset values (latest offset, commited offset, and lag) for each
 * partition of the given Kafka topic for the given consumer group
 * 
 * @author jaeminsung
 * @version 1.0
 * @since 3/26/2016
 */
@Component
public class KafkaOffsetTracker {
	
	private final Logger LOGGER = LoggerFactory.getLogger(KafkaOffsetTracker.class);
	private final int REQUEST_TIMEOUT = 6000;
	private final int SESSION_TIMEOUT = 5000;
	private String topic, group;
	private Properties props;
	
	/**
	 * initialize() prepares necessary field variables and Kafka consumer properties
	 * 
	 * @param brokers
	 * @param topic
	 * @param group
	 */
	private void initialize(String brokers, String topic, String group) {
		Validate.notEmpty(brokers);
		Validate.notEmpty(topic);
		Validate.notEmpty(group);
		
		this.topic = topic;
		this.group = group;
		
		props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", group);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("request.timeout.ms", REQUEST_TIMEOUT); //timeout for client waiting for response
		props.put("session.timeout.ms", SESSION_TIMEOUT); //timeout used to detect Kafka failures
	}
	
	/**
	 * getOffsetInfoMap() fetches offsets (latest offset and committed offset) for the given topic
	 * and consumer group by connecting to the given Kafka brokers
	 * 
	 * @param brokers
	 * @param topic
	 * @param group
	 * @return Map of Partition and Offsets
	 */
	public Map<Integer, OffsetInfo> getOffsetInfoMap(String brokers, String topic, String group) {
		initialize(brokers, topic, group);
		Map<Integer, OffsetInfo> offsetInfoMap = new HashMap<Integer, OffsetInfo>();
		
		// Get valid partitions for the given Kafka topic
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		List<PartitionInfo> partitions;
		try {
			partitions = consumer.partitionsFor(topic);
		} catch(TimeoutException e) {
			throw new TimeoutException(
					String.format("Unable to connect to Kafka broker(s) [%s]", brokers));
		}
		
		// PartitionInfo is null if an invalid topic name is provided
		if (partitions == null) {
			consumer.close();
			throw new InvalidTopicException(
					String.format("[%s] is not a valid topic", topic));
		} 
		
		// Get offset info for each partition
		partitions.forEach((x) -> {
			offsetInfoMap.put(Integer.valueOf(x.partition()), getOffsetInfo(consumer, x));
		});

		consumer.close();

		return offsetInfoMap;
	}
	
	/**
	 * getOffsetInfo() retrieves offsets and calculates lag for the given partition
	 * 
	 * @param consumer
	 * @param pInfo
	 * @return OffsetInfo containing latest offset, committed offset, and lag
	 */
	private OffsetInfo getOffsetInfo(KafkaConsumer<String, String> consumer, PartitionInfo pInfo) {
		OffsetAndMetadata offsetData = consumer.committed(new TopicPartition(topic, pInfo.partition()));

		if (offsetData == null) {
			consumer.close();
			throw new RuntimeException(
					String.format("No committed offsets exist for the consumer group [%s] "
							+ "and topic [%s]", group, topic));
		}

		long latestOffset = getLatestOffset(pInfo);
		long committedOffset = offsetData.offset();

		return new OffsetInfo(latestOffset, committedOffset);
	}
	
	/**
	 * getLatestOffset() utilizes SimpleConsumer (Kafka's low-level consumer)
	 * to fetch the latest available offset for the given partition
	 * 
	 * @param partitionInfo
	 * @return latest offset
	 */
	private long getLatestOffset(PartitionInfo partitionInfo) {
		final int SOCKET_TIMEOUT = 100000;
		final int BUFFER_SIZE = 64 * 1024;
		final String CLIENT = "offset_lookup";
		final long LATEST = -1L; // -1 indicates the latest offset; -2 indicates the earliest
		final int MAX_OFFSET_NUM = 1;
		
		String p_topic = partitionInfo.topic();
		int p_id = partitionInfo.partition();
		Node leader = partitionInfo.leader();
		
		// Build an OffsetRequest to get the latest offset for the given topic and partition
		TopicAndPartition topicAndPartition = new TopicAndPartition(p_topic, p_id);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = 
										new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(LATEST, MAX_OFFSET_NUM));
		OffsetRequest offsetRequest = new OffsetRequest(requestInfo,
														kafka.api.OffsetRequest.CurrentVersion(),
														CLIENT);
		
		// Use SimpleConsumer to connect to Kafka brokers and send an OffsetRequest
		SimpleConsumer simpleConsumer = new SimpleConsumer(leader.host(), leader.port(), 
														   SOCKET_TIMEOUT, BUFFER_SIZE, CLIENT);
		OffsetResponse response = simpleConsumer.getOffsetsBefore(offsetRequest);
		simpleConsumer.close();

		if (response.hasError()) {
			LOGGER.error(
					String.format("Error in getting the latest offset for [%s, %s]. "
					+ "Refer to the error code: %d", topic, partitionInfo.partition(), (short) 12));
			return -1L;
		} else {
			long[] offsets = response.offsets(p_topic, p_id);
			return offsets[0];
		}
	}
}
