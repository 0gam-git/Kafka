package kafka.book.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.BasicConfigurator;

public class ProducerSample {

	public static void main(String[] args) {
		BasicConfigurator.configure();

		ProducerSample sample = new ProducerSample();
		sample.startProducerBySync();
	}

	public void startProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "203.251.177.28:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		try {
			producer.send(new ProducerRecord<String, String>("peter-topic",
					"Apache Kafka is a distributed streaming platform"));
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			producer.close();
		}
	}

	public void startProducerBySync() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "203.251.177.28:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		try {
			RecordMetadata metadata = producer.send(new ProducerRecord<String, String>("peter-topic",
					"Apache Kafka is a distributed streaming platform")).get();

			// point.
			System.out.printf("Partition : %d, Offset : %d", metadata.partition(), metadata.offset());

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			producer.close();
		}
	}

	public void startProducerByAsync() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "203.251.177.28:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		try {
			producer.send(new ProducerRecord<String, String>("peter-topic",
					"Apache Kafka is a distributed streaming platform"), new ProducerCallBack());

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			producer.close();
		}
	}

}
