package vn.motdev.kafkaerror;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Demo Buổi 9 — hai trường phái xử lý lỗi ở consumer, chạy song song để so sánh:
 *
 * <ul>
 *   <li>topic {@code payments}: retry BLOCKING bằng {@code DefaultErrorHandler} + backoff,
 *       hết retry thì {@code DeadLetterPublishingRecoverer} đẩy sang retry topic hoặc DLT;</li>
 *   <li>topic {@code orders}: retry NON-BLOCKING bằng {@code @RetryableTopic} — Spring tự sinh
 *       các topic retry, luồng chính không bị tắc.</li>
 * </ul>
 *
 * <p>@EnableScheduling phục vụ phần xử lý lỗi phía PRODUCER: event gửi hỏng được cất lại và
 * scheduler định kỳ gửi lại.
 */
@SpringBootApplication
@EnableScheduling
public class KafkaErrorDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaErrorDemoApplication.class, args);
    }
}
