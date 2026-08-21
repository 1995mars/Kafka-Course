package vn.motdev.kafkaerror.producer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Xử lý lỗi phía PRODUCER.
 *
 * <p>Producer hỏng khi nào? Cluster sập; {@code acks=all} mà số replica sống không đủ
 * {@code min.insync.replicas}; topic không tồn tại; mạng đứt. Lúc đó CompletableFuture trả về
 * exception — bỏ qua nó là mất event trong im lặng.
 *
 * <p>Vì sao KHÔNG đẩy event hỏng vào một "retry topic"? Vì lý do làm nó hỏng thường là cả
 * cluster có vấn đề — retry topic nằm trên chính cluster đó thì cũng chết theo. Đáng tin hơn:
 * lưu event vào **database** rồi để scheduler gửi lại. Hàng đợi trong bộ nhớ dưới đây là bản
 * rút gọn cho demo — mất sạch nếu app restart; bản đầy đủ chính là Transaction Outbox ở Buổi 11.
 */
@Component
public class ResilientEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ResilientEventPublisher.class);
    private static final int MAX_RESEND_ATTEMPTS = 3;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Queue<PendingEvent> pending = new ConcurrentLinkedQueue<>();

    public ResilientEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String topic, String key, String payload) {
        send(new PendingEvent(topic, key, payload, 0));
    }

    private void send(PendingEvent event) {
        kafkaTemplate.send(event.topic(), event.key(), event.payload()).whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[PUBLISH ] {} → {} partition={}",
                        event.payload(), event.topic(), result.getRecordMetadata().partition());
                return;
            }
            if (event.attempt() >= MAX_RESEND_ATTEMPTS) {
                log.error("[PUBLISH ] ☠ bỏ cuộc với {} sau {} lần gửi lại — cần can thiệp thủ công",
                        event.payload(), event.attempt());
                return;
            }
            // ngoài đời: INSERT vào bảng outbox trong CÙNG transaction với thay đổi nghiệp vụ
            pending.add(event.nextAttempt());
            log.error("[PUBLISH ] gửi {} thất bại ({}) → cất lại để gửi lần {}",
                    event.payload(), ex.getMessage(), event.attempt() + 1);
        });
    }

    /** Scheduler quét hàng chờ và gửi lại — thay cho việc để producer tự retry vô hạn. */
    @Scheduled(fixedDelay = 10_000)
    public void resendPending() {
        // Chỉ lấy đúng số phần tử đang có lúc bắt đầu: callback thất bại chạy trên thread khác
        // và nạp lại vào hàng chờ, vét kiểu while-poll có thể quay vòng ngay trong một lượt quét.
        for (int remaining = pending.size(); remaining > 0; remaining--) {
            PendingEvent event = pending.poll();
            if (event == null) {
                return;
            }
            log.warn("[PUBLISH ] gửi lại {} (lần {})", event.payload(), event.attempt());
            send(event);
        }
    }

    private record PendingEvent(String topic, String key, String payload, int attempt) {
        PendingEvent nextAttempt() {
            return new PendingEvent(topic, key, payload, attempt + 1);
        }
    }
}
