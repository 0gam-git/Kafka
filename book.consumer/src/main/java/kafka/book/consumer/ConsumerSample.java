package kafka.book.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.BasicConfigurator;

/**
 * - 컨슈머 토픽 파티션 (1) : 컨슈머 (1) 로 연결된다.
 * 
 * - 컨슈머 그룹 컨슈머 그룹 내의 컨슈머들은 메시지를 가져오고 있는 토픽의 파티션에 대해 소유권을 공유한다.
 * 
 * - 오프셋, 커밋, 컨슈머 그룹의 중요성
 * 
 */
public class ConsumerSample {

	public static void main(String[] args) {
		BasicConfigurator.configure();

		Properties props = new Properties();
		props.put("bootstrap.servers", "203.251.177.28:9092");
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		ConsumerSample sample = new ConsumerSample();
		sample.startAutoCommit(props);

	}

	public void startAutoCommit(Properties props) {
		props.put("group.id", "peter-auto");
		props.put("enable.auto.commit", "true");

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

	public void startManualCommit(Properties props) {
		props.put("group.id", "peter-manual");
		props.put("enable.auto.commit", "false");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("peter-topic"));

		try {

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
							record.value());
				}

				try {
					// point.
					consumer.commitSync();

				} catch (CommitFailedException e) {
					System.out.printf("error : ", e);
				}
			}

		} finally {
			consumer.close();
		}
	}

	public void startManualCommitByPartition(Properties props) {
		props.put("group.id", "peter-partition");
		props.put("enable.auto.commit", "false");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		String topic = "peter-topic";

		// point.
		TopicPartition partition0 = new TopicPartition(topic, 0);
		TopicPartition partition1 = new TopicPartition(topic, 1);

		consumer.assign(Arrays.asList(partition0, partition1));

		consumer.seek(partition0, 2);
		consumer.seek(partition1, 2);

		try {

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
							record.value());
				}

				try {
					// point.
					consumer.commitSync();

				} catch (CommitFailedException e) {
					System.out.printf("error : ", e);
				}
			}

		} finally {
			consumer.close();
		}
	}

}
