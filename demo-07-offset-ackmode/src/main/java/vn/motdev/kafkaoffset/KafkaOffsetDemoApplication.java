package vn.motdev.kafkaoffset;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Demo Buổi 7 — commit offset được thực hiện KHI NÀO, và ai quyết định.
 *
 * <p>Ba nhóm nội dung chạy được trong project này:
 * <ul>
 *   <li>profile mặc định: ba listener với ba AckMode (BATCH / RECORD / MANUAL_IMMEDIATE)
 *       cùng đọc topic orders bằng ba group khác nhau để so sánh;</li>
 *   <li>profile {@code pitfall}: cạm bẫy đẩy việc sang thread pool rồi return sớm;</li>
 *   <li>profile {@code plain-client}: commit bằng Kafka client thuần —
 *       auto / commitSync / commitAsync / commit offset cụ thể.</li>
 * </ul>
 */
@SpringBootApplication
public class KafkaOffsetDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaOffsetDemoApplication.class, args);
    }
}
