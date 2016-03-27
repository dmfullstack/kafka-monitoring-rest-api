package com.jaeminsung.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.jaeminsung.kafka.KafkaOffsetTracker;

public class KafkaMonitoringControllerTest {
	
	@InjectMocks
	private KafkaMonitoringController controller;
	
	private MockMvc mockMvc;

	@Test
	public void testKafkaMonitoringController() throws Exception {
        String brokers = "localhost:9092";
        String topic = "test_topic";
        String group = "test_group";
		
		MockitoAnnotations.initMocks(this);
        KafkaOffsetTracker offsetTrackerMock = mock(KafkaOffsetTracker.class);
        when(offsetTrackerMock.getOffsetInfoMap(brokers, topic, group)).thenReturn(null);
       
        ReflectionTestUtils.setField(controller, "tracker", offsetTrackerMock);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
        
        mockMvc.perform(
                get("/offsets?brokers={brokers}&topic={topic}&group={group}", brokers, topic, group))
                .andExpect(MockMvcResultMatchers.status().isOk());
	}
}
