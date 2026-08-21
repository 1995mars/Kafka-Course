package vn.motdev.kafkatesting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Ứng dụng cực gọn để làm đối tượng test ở Buổi 10: một producer, một consumer. */
@SpringBootApplication
public class KafkaTestingDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTestingDemoApplication.class, args);
    }
}
