package kafka.study;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

    private static final String PROPERTIES_FILE = "log-service.properties";

    public static void main(String[] args) {
        Properties properties = PropertiesLoader.fromFile(PROPERTIES_FILE);
        var consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Pattern.compile(Topic.ECOMMERCE.name() + ".*"));

        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("------------------------------------------");
                System.out.println("Records found: " + records.count());
                for (var record : records) {
                    System.out.println("[INFO] " + record.toString());
                }
            }
        }
    }

}
