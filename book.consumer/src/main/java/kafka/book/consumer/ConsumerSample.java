package kafka.book.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.BasicConfigurator;

public class ConsumerSample {

	public static void main(String[] args) {
		BasicConfigurator.configure();
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "203.251.177.28:9092");
		props.put("group.id", "peter-consumer");
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("peter-topic"));

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
							record.value());
				}
			}

		} finally {
			consumer.close();
		}
	}

}
