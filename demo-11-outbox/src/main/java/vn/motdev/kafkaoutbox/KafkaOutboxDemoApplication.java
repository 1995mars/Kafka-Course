package vn.motdev.kafkaoutbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Demo Buổi 11 — Message Delivery Guarantees: pipeline không mất, không trùng.
 *
 * <p>Ba mảnh ghép, mỗi mảnh chặn một cửa mất/trùng dữ liệu:
 * <ol>
 *   <li><b>Transaction Outbox</b> — message ghi vào bảng outbox CÙNG transaction với dữ liệu
 *       nghiệp vụ, nên không bao giờ có chuyện "DB rollback mà Kafka vẫn nhận" (hay ngược lại).</li>
 *   <li><b>Idempotent + transactional producer</b> — relay retry thoải mái, broker vẫn chỉ
 *       ghi một bản; consumer đọc read_committed.</li>
 *   <li><b>Idempotent consumer</b> — nếu relay gửi lại (at-least once mà), consumer tra
 *       eventId đã xử lý và bỏ qua bản trùng.</li>
 * </ol>
 *
 * <p>{@code @EnableScheduling} cho Message Relay quét bảng outbox định kỳ.
 */
@SpringBootApplication
@EnableScheduling
public class KafkaOutboxDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaOutboxDemoApplication.class, args);
    }
}
