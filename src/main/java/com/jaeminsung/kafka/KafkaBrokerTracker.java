package com.jaeminsung.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.stereotype.Component;

/**
 * KafkaBrokerTracker provides a method checkBrokersAlive() to verify
 * whether each of the input Kafka brokers are up and running or not
 * 
 * @author jsung
 * @version 1.0
 * @since 4/30/2016
 *
 */
@Component
public class KafkaBrokerTracker {
	
	private final static int REQUEST_TIMEOUT = 600; // Timeout duration for checking a broker's status
	private final static int SESSION_TIMEOUT = 300;
	private final static int HEARTBEAT_TIMEOUT = 100; // about 1/3 of the value of Session Timeout 
	private static Properties props;
	
	static {
		props = new Properties();
		props.put("bootstrap.servers", "");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("request.timeout.ms", REQUEST_TIMEOUT); //timeout for client waiting for response
		props.put("session.timeout.ms", SESSION_TIMEOUT); //timeout used to detect Kafka failures
		props.put("heartbeat.interval.ms", HEARTBEAT_TIMEOUT);
	}
	
	/**
	 * checkBrokersAlive() gets info on whether each Kafka broker is up and running (true)
	 * or not (false)
	 * 
	 * @param brokers
	 * @return Key-Value map where key is Kafka broker and value is true/false
	 */
	public static Map<String, Boolean> checkBrokersAlive(String brokers) {
		return new HashMap<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			{
				String[] brokerArray = brokers.split(",");
				Arrays.asList(brokerArray).forEach((broker) -> {
					try {
						put(broker, isBrokerAlive(broker));
					} catch (KafkaException e) {
						throw new KafkaException(String.format("Invalid broker address--%s", broker));
					}
				});
			}
		};
	}
	
	/**
	 * isBrokerAlive() uses KafkaConsumer to verify whether the given Kafka
	 * broker is up and running or not
	 * 
	 * @param broker
	 * @return true (broker is up) or false (broker is down)
	 * @throws KafkaException when an invalid Kafka broker address is provided
	 */
	private static boolean isBrokerAlive(String broker) throws KafkaException {
		boolean alive = false;
		props.put("bootstrap.servers", broker);
		
		KafkaConsumer<String, String> consumer = null;
		try {
			consumer = new KafkaConsumer<>(props);
			consumer.listTopics();
			alive = true;
		} catch (TimeoutException e) {
			//Do nothing
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}

		return alive;
	}	
}
