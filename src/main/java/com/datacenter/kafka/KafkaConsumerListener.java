package com.datacenter.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;
import java.util.Optional;
/**
 * kafka消费者
 */
public class KafkaConsumerListener {


    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerListener.class);

    @Autowired
    com.deppon.datacenter.kafka.ScheduledService scheduledService;

    /**
     * 消费kafka数据
     * @param records
     */
    @KafkaListener(topics = "#{'${spring.kafka.topic.name}'.split(',')}")
    public void listener(List<ConsumerRecord<String, String>> records) {
        try {
            for (ConsumerRecord<String, String> record : records) {
                Optional<?> kafkaMessage = Optional.ofNullable(record.value());
                LOG.info("Received: " + record);
                if (kafkaMessage.isPresent()) {
                    scheduledService.kafkaToTidb(record.value());
                }
            }
        } catch (Exception e) {
            LOG.error("consumer error ..."+e.getMessage());
        }
    }











}
