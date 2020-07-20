package kafka.study;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

    private static final String PROPERTIES_FILE = "email-service.properties";

    public static void main(String[] args) throws InterruptedException {
        Properties properties = PropertiesLoader.fromFile(PROPERTIES_FILE);
        var consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(Topic.ECOMMERCE__SEND_EMAIL.name()));

        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("------------------------------------------");
                System.out.println("Records found: " + records.count());
                for (var record : records) {
                    System.out.println("Sending email: " + record.toString());

                    // Simulate mail sending duration
                    Thread.sleep(500);
                    System.out.println("Email sent");
                }
            }
        }
    }

}
