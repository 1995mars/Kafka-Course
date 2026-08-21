package vn.motdev.kafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Ứng dụng Kafka đầu tiên — phía NHẬN (Buổi 6).
 *
 * <p>Không cần annotation nào thêm: @SpringBootApplication đã kéo theo
 * KafkaAutoConfiguration, và chính nó bật @EnableKafka để các @KafkaListener hoạt động.
 */
@SpringBootApplication
public class KafkaConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }
}
