package kafka;

import java.util.Properties;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class SimpleConsumer {
   public static void main(String[] args) throws Exception {
     
      //Kafka consumer configuration settings
      //String topicName = "MARKS_DATA";
      Properties props = new Properties();
      
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "testhdfs");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.setProperty("auto.offset.reset", "earliest");
      //props.setProperty("max.poll.records", "2");
      
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList("test_hdfs"));
      
      
      //print the topic name
      //System.out.println("Subscribed to topic " + topicName);
      int i = 0;
      Scanner scanner = new Scanner(System.in);
      while (true) {
    	  
         ConsumerRecords<String, String> records = consumer.poll(5);
         //String input = scanner.nextLine();
        // System.out.println(records.count());
         //consumer.seek(new TopicPartition("MARKS_DATA", 0),Long.parseLong("2"));
         for (ConsumerRecord<String, String> record : records)
         
         {// print the offset,key and value for the consumer records.
         System.out.printf("offset = %d, key = %s, value = %s\n", 
            record.offset(), record.key(), record.value());
         System.out.println(record.partition());
         }
         i++;
      }
      
   }
}