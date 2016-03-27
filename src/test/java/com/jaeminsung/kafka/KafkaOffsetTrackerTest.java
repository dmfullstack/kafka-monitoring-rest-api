package com.jaeminsung.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.jaeminsung.domain.OffsetInfo;

public class KafkaOffsetTrackerTest {
	
	private final static String BROKERS = "localhost:9022,localhost:9093,localhost:9094";
	private final static String INVALID_BROKERS = "localhost:9095,localhost:9096";
	private final static String TOPIC = "test_topic";
	private final static String INVALID_TOPIC = "wrong_topic";
	private final static String GROUP = "test_group";
	private final static String INVALID_GROUP = "wrong_group";
	
	private static KafkaOffsetTracker tracker;
	
	@BeforeClass
	public static void setUp() {
		OffsetInfo offsetInfoPart0 = new OffsetInfo(2L, 2L);
		OffsetInfo offsetInfoPart1 = new OffsetInfo(100L, 90L);
		Map<Integer, OffsetInfo> offsetInfoMap = new HashMap<Integer, OffsetInfo>();
		offsetInfoMap.put(Integer.valueOf(0), offsetInfoPart0);
		offsetInfoMap.put(Integer.valueOf(1), offsetInfoPart1);
		
		tracker = mock(KafkaOffsetTracker.class);
		doReturn(offsetInfoMap).when(tracker).getOffsetInfoMap(BROKERS, TOPIC, GROUP);
		doThrow(new TimeoutException()).when(tracker).getOffsetInfoMap(INVALID_BROKERS, TOPIC, GROUP);
		doThrow(new InvalidTopicException()).when(tracker).getOffsetInfoMap(BROKERS, INVALID_TOPIC, GROUP);
		doThrow(new RuntimeException()).when(tracker).getOffsetInfoMap(BROKERS, TOPIC, INVALID_GROUP);
	}

	@Test
	public void testKafkaOffsetTracker() {
		Map<Integer, OffsetInfo> mockedMap = tracker.getOffsetInfoMap(BROKERS, TOPIC, GROUP);
		assertNotNull(mockedMap);
		
		OffsetInfo part0 = mockedMap.get(Integer.valueOf(0));
		OffsetInfo part1 = mockedMap.get(Integer.valueOf(1));
		assertNotNull(part0);
		assertNotNull(part1);

		assertEquals(0L, part0.getLag());
		assertEquals(10L, part1.getLag());
	}
	
	@Test(expected=TimeoutException.class)
	public void testKafkaOffsetTrackerWithInvalidBrokers() {
		tracker.getOffsetInfoMap(INVALID_BROKERS, TOPIC, GROUP);
	}
	
	@Test(expected=InvalidTopicException.class)
	public void testKafkaOffsetTrackerWithInvalidTopic() {
		tracker.getOffsetInfoMap(BROKERS, INVALID_TOPIC, GROUP);
	}
	
	@Test(expected=RuntimeException.class)
	public void testKafkaOffsetTrackerWithInvalidGroup() {
		tracker.getOffsetInfoMap(BROKERS, TOPIC, INVALID_GROUP);
	}
}
