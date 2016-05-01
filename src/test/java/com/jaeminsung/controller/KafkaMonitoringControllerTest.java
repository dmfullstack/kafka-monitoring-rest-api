package com.jaeminsung.controller;

import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.jaeminsung.domain.OffsetInfo;
import com.jaeminsung.kafka.KafkaBrokerTracker;
import com.jaeminsung.kafka.KafkaOffsetTracker;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaOffsetTracker.class, KafkaBrokerTracker.class})
public class KafkaMonitoringControllerTest {
	
	private static String brokers, topic, group;
	private static Map<Integer,OffsetInfo> offsets;
	private static Map<String, Boolean> brokerStatus;

	@InjectMocks
	private KafkaMonitoringController controller;
	
	@BeforeClass
	public static void setUp() {
	    brokers = "localhost:9092";
	    topic = "test_topic";
	    group = "test_group";
	    
        offsets = new HashMap<Integer,OffsetInfo>();
		OffsetInfo info = new OffsetInfo(2L, 0L);
		offsets.put(0, info);
		
        brokerStatus = new HashMap<String, Boolean>();
		brokerStatus.put(brokers, true);
	}

	@Test
	public void testKafkaMonitoringControllerOffsetTracker() throws Exception {
		MockitoAnnotations.initMocks(this);
		mockStatic(KafkaOffsetTracker.class);
        expect(KafkaOffsetTracker.getOffsetInfoMap(brokers, topic, group)).andReturn(offsets);
        replay(KafkaOffsetTracker.class);

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
        mockMvc.perform(
                get("/offsets?brokers={brokers}&topic={topic}&group={group}", brokers, topic, group))
                .andExpect(MockMvcResultMatchers.status().isOk());
        
        verify(KafkaOffsetTracker.class);
	}
	
	@Test
	public void testKafkaMonitoringControllerBrokerTracker() throws Exception {
		MockitoAnnotations.initMocks(this);
		mockStatic(KafkaBrokerTracker.class);
        expect(KafkaBrokerTracker.checkBrokersAlive(brokers)).andReturn(brokerStatus);
        replay(KafkaBrokerTracker.class);

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
        mockMvc.perform(
                get("/checkBrokerStatus?brokers={brokers}", brokers))
                .andExpect(MockMvcResultMatchers.status().isOk());
        
        verify(KafkaBrokerTracker.class);
	}
}
