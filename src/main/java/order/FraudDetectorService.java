package order;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

public class FraudDetectorService {
    public static void main(String[] args) {
        var properties = new MyProperties();
        var consumer = new KafkaConsumer<String, String>(properties.Myproperties());
        consumer.subscribe(Collections.singletonList("ECORMMECE_NEW_ORDER"));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Found"+ records.count() +"data");
                for (var record : records) {
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    System.out.println("Order processing");
                }
            }
        }


    }
}
