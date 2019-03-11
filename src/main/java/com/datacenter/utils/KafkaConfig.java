package com.datacenter.utils;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String  kafkaService;

    @Value("${spring.kafka.consumer.group-id}")
    private String  groupId;

    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private String  autoCommit;
    @Value("${kafka.consumer.auto.commit.interval.ms}")
    private String  intervalMs;
    @Value("${kafka.consumer.session.timeout.ms}")
    private String  timeoutMs;
    @Value("${MAX.POLL.RECORDS.CONFIG}")
    private String  pollRecords;
    @Value("${spring.kafka.consumer.key-deserializer}")
    private String  deserializer;

    @Value("${spring.kafka.consumer.concurrency.count}")
    private Integer  concurrencyCount;

    public Integer getConcurrencyCount() {
        return concurrencyCount;
    }

    public void setConcurrencyCount(Integer concurrencyCount) {
        this.concurrencyCount = concurrencyCount;
    }

    public String getKafkaService() {
        return kafkaService;
    }

    public void setKafkaService(String kafkaService) {
        this.kafkaService = kafkaService;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(String autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getIntervalMs() {
        return intervalMs;
    }

    public void setIntervalMs(String intervalMs) {
        this.intervalMs = intervalMs;
    }

    public String getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(String timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public String getDeserializer() {
        return deserializer;
    }

    public void setDeserializer(String deserializer) {
        this.deserializer = deserializer;
    }

    public String getPollRecords() {
        return pollRecords;
    }

    public void setPollRecords(String pollRecords) {
        this.pollRecords = pollRecords;
    }
}
