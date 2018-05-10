package kafka.book.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerCallBack implements Callback {

	public void onCompletion(RecordMetadata metadata, Exception e) {

		if (metadata != null) {
			System.out.println("Partition : " + metadata.partition() + "Offset : " + metadata.offset() + "");

		} else {
			e.printStackTrace();
		}

	}

}
