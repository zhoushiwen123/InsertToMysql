package com.datacenter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaProducerTest extends DataCenterTidbApplicationTests{


    private static List<String> list = new ArrayList<>();
    @Test
    public void testSendKafka() {
        txt2String(new File("/Users/zhoushiwen/JavaDev/git/DataCenter/DataCenterTidb/src/test/java/com/deppon/datacenter/table_json.txt"));
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.224.64.62:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        System.out.println(list.size());
        for(int i = 0;i<1000;i++){
            for (String value: list) {
                producer.send(new ProducerRecord<String, String>("test", value));
                producer.send(new ProducerRecord<String, String>("test1", value));
            }
        }
        producer.close();
    }


    /**
     * 读取txt文件的内容
     * @param file 想要读取的文件对象
     * @return 返回文件内容
     */
    public static String txt2String(File file){
        StringBuilder result = new StringBuilder();
        try{
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
//                result.append(System.lineSeparator()+s);
                list.add(s);
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return result.toString();
    }


}