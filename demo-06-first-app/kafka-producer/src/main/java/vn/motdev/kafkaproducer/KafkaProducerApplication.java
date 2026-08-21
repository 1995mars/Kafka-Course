package vn.motdev.kafkaproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Ứng dụng Kafka đầu tiên — phía GỬI (Buổi 6).
 *
 * <p>@EnableScheduling là thứ duy nhất phải thêm ngoài @SpringBootApplication: nó bật
 * cơ chế @Scheduled để {@link RandomNumberProducer} chạy mỗi giây.
 *
 * <p>Ứng dụng không có web starter nhưng vẫn sống: thread của task scheduler là
 * non-daemon, JVM chỉ thoát khi ta dừng bằng Ctrl+C.
 */
@SpringBootApplication
@EnableScheduling
public class KafkaProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApplication.class, args);
    }
}
