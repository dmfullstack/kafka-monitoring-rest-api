package com.jaeminsung.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.jaeminsung.domain.OffsetInfo;
import com.jaeminsung.kafka.KafkaOffsetTracker;

/**
 * KafkaMonitoringController is a RESTful API controller that serves
 * GET requests for fetching Kafka offset info
 * 
 * @author jaeminsung
 * @version 1.0
 * @since 3/26/2016
 */
@RestController
public class KafkaMonitoringController {
	
	@Autowired
	private KafkaOffsetTracker tracker;
    
    @RequestMapping(value = "/offsets", method = RequestMethod.GET)
    public Map<Integer,OffsetInfo> offsets(@RequestParam("topic") String topic,
    									   @RequestParam("group") String group,
    									   @RequestParam("brokers") String brokers) {
        return tracker.getOffsetInfoMap(brokers, topic, group);
    }
}