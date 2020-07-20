package kafka.study;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

public class FraudDetectorService {

    private static final String PROPERTIES_FILE = "fraud-detector-service.properties";

    public static void main(String[] args) throws InterruptedException {
        var properties = PropertiesLoader.fromFile(PROPERTIES_FILE);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getSimpleName() + "-" + UUID.randomUUID().toString());

        var consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(Topic.ECOMMERCE__NEW_ORDER.name()));
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("------------------------------------------");
                System.out.println("Records found: " + records.count());
                for (var record : records) {
                    System.out.println("Checking for fraud: " + record.toString());

                    // Simulate fraud check duration
                    Thread.sleep(3000);
                    System.out.println("Order processed");
                }
            }
        }
    }

}
