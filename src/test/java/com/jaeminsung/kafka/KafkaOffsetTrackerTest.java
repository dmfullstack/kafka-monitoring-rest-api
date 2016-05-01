package com.jaeminsung.kafka;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.jaeminsung.domain.OffsetInfo;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaOffsetTracker.class, KafkaBrokerTracker.class})
public class KafkaOffsetTrackerTest {
	
	private final static String BROKERS = "localhost:9022,localhost:9093,localhost:9094";
	private final static String INVALID_BROKERS = "localhost:9095,localhost:9096";
	private final static String TOPIC = "test_topic";
	private final static String INVALID_TOPIC = "wrong_topic";
	private final static String GROUP = "test_group";
	private final static String INVALID_GROUP = "wrong_group";
	
	@Test
	public void testKafkaOffsetTracker() {
		OffsetInfo offsetInfoPart0 = new OffsetInfo(2L, 2L);
		OffsetInfo offsetInfoPart1 = new OffsetInfo(100L, 90L);
		Map<Integer, OffsetInfo> offsetInfoMap = new HashMap<Integer, OffsetInfo>();
		offsetInfoMap.put(Integer.valueOf(0), offsetInfoPart0);
		offsetInfoMap.put(Integer.valueOf(1), offsetInfoPart1);
		
		mockStatic(KafkaOffsetTracker.class);
        expect(KafkaOffsetTracker.getOffsetInfoMap(BROKERS, TOPIC, GROUP)).andReturn(offsetInfoMap);
        replay(KafkaOffsetTracker.class);
		
		Map<Integer, OffsetInfo> mockedMap = KafkaOffsetTracker.getOffsetInfoMap(BROKERS, TOPIC, GROUP);
		verify(KafkaOffsetTracker.class);
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
		mockStatic(KafkaOffsetTracker.class);
		expect(KafkaOffsetTracker.getOffsetInfoMap(INVALID_BROKERS, TOPIC, GROUP)).andThrow(new TimeoutException());
		replay(KafkaOffsetTracker.class);
		
		KafkaOffsetTracker.getOffsetInfoMap(INVALID_BROKERS, TOPIC, GROUP);
		verify(KafkaOffsetTracker.class);
	}
	
	@Test(expected=InvalidTopicException.class)
	public void testKafkaOffsetTrackerWithInvalidTopic() {
		mockStatic(KafkaOffsetTracker.class);
		expect(KafkaOffsetTracker.getOffsetInfoMap(BROKERS, INVALID_TOPIC, GROUP)).andThrow(new InvalidTopicException());
		replay(KafkaOffsetTracker.class);
		
		KafkaOffsetTracker.getOffsetInfoMap(BROKERS, INVALID_TOPIC, GROUP);
		verify(KafkaOffsetTracker.class);
	}
	
	@Test(expected=RuntimeException.class)
	public void testKafkaOffsetTrackerWithInvalidGroup() {
		mockStatic(KafkaOffsetTracker.class);
		expect(KafkaOffsetTracker.getOffsetInfoMap(BROKERS, TOPIC, INVALID_GROUP)).andThrow(new RuntimeException());
		replay(KafkaOffsetTracker.class);
		
		KafkaOffsetTracker.getOffsetInfoMap(BROKERS, TOPIC, INVALID_GROUP);
		verify(KafkaOffsetTracker.class);
	}
}
