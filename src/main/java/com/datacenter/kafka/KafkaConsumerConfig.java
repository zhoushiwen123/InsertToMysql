package com.datacenter.kafka;

import com.deppon.datacenter.utils.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: zsw
 * @date:
 * @description kafka
 */
@Component
@Configuration
@EnableKafka
public class KafkaConsumerConfig {
    public KafkaConsumerConfig(){
        System.out.println("kafka消费者配置加载...");
    }


    @Autowired
    KafkaConfig kafkaConfig;

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(kafkaConfig.getConcurrencyCount());
        /**
         * 批量消费
         */
        factory.setBatchListener(true);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory(consumerProperties());
    }

    /**
     * kafka基本参数
     * @return
     */
    @Bean
    public Map<String, Object> consumerProperties() {
        Map<String, Object> props= new HashMap<String, Object>();

        //kafka地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaService());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,  kafkaConfig.getGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,  kafkaConfig.getAutoCommit());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaConfig.getIntervalMs());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,  kafkaConfig.getTimeoutMs());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  kafkaConfig.getDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,   kafkaConfig.getDeserializer());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,kafkaConfig.getPollRecords());
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,kafkaConfig.getTimeoutMs());
       return props;
    }

    /**
     * 自己实现的消费逻辑
     * @return
     */
    @Bean
    public KafkaConsumerListener kafkaConsumerListener(){
        return new KafkaConsumerListener();
    }

}