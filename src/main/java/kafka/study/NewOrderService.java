package kafka.study;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderService {

	private static final String PROPERTIES_FILE = "new-order-service.properties";
	private static final int ORDER_AMOUNT = 10;

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties properties = PropertiesLoader.fromFile(PROPERTIES_FILE);
		var producer = new KafkaProducer<String, String>(properties);

		for (var i = 0; i < ORDER_AMOUNT; i++) {
            var key = UUID.randomUUID().toString();

			var orderRecord = new ProducerRecord<>(Topic.ECOMMERCE__NEW_ORDER.name(), key, buildOrder(key));
            producer.send(orderRecord, buildCallback()).get();

			var emailRecord = new ProducerRecord<>(Topic.ECOMMERCE__SEND_EMAIL.name(), key, buildEmail());
			producer.send(emailRecord, buildCallback()).get();
		}
	}

	private static String buildOrder(String key) {
		return String.format("%s,%s,%s", key, UUID.randomUUID().toString(), UUID.randomUUID().toString());
	}

    private static String buildEmail() {
        return "Thank you for your order! We are processing your order!";
    }

	private static Callback buildCallback() {
		return (data, ex) -> {
			if (ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println("Success " + data.topic()
                    + ":::partition " + data.partition()
                    + " | offset " + data.offset()
                    + " | timestamp " + data.timestamp());
		};
	}

}
