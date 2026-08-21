package vn.motdev.kafkaerror.order;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Cách hiện đại và gọn nhất: {@code @RetryableTopic} — retry NON-BLOCKING.
 *
 * <p>Khác biệt cốt lõi so với {@code DefaultErrorHandler}: message hỏng được ĐẨY SANG topic
 * retry riêng (orders-retry-0, orders-retry-1, …) do Spring tự sinh, nên partition của topic
 * chính chạy tiếp ngay, không đứng chờ backoff.
 *
 * <p>Ý nghĩa các tham số:
 * <ul>
 *   <li>{@code attempts = "4"}: 1 lượt trên topic chính + 3 lượt trên các retry topic, hết thì vào DLT;</li>
 *   <li>{@code backoff}: 1s → 2s → 4s (nhân đôi mỗi lần);</li>
 *   <li>{@code include}: CHỈ những exception này mới được retry — thứ khác đi thẳng DLT;</li>
 *   <li>{@code dltStrategy = FAIL_ON_ERROR}: vào DLT là dừng, không retry tiếp;</li>
 *   <li>{@code autoCreateTopics = "true"}: tiện cho môi trường test; production tạo topic chủ động.</li>
 * </ul>
 */
@Component
public class OrderRetryableTopicConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderRetryableTopicConsumer.class);

    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            include = RecoverableDataAccessException.class,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            autoCreateTopics = "true")
    @KafkaListener(topics = "${app.topics.orders}", groupId = "order-service")
    public void consume(ConsumerRecord<String, String> record) {
        String order = record.value();
        int attempt = attempts.computeIfAbsent(order, key -> new AtomicInteger()).incrementAndGet();
        log.info("[ORDER   ] nhận {} tại topic {} (lần thử {})", order, record.topic(), attempt);

        if (order.startsWith("invalid")) {
            // không nằm trong include → bỏ qua toàn bộ retry, vào thẳng orders-dlt
            throw new IllegalArgumentException("đơn hàng sai định dạng: " + order);
        }
        if (order.startsWith("fail")) {
            throw new RecoverableDataAccessException("kho hàng không phản hồi, lần thử " + attempt);
        }
        if (order.startsWith("flaky") && attempt <= 2) {
            throw new RecoverableDataAccessException("mạng chập chờn, lần thử " + attempt);
        }

        log.info("[ORDER   ] ✅ xử lý thành công {} (lần thử {})", order, attempt);
    }

    /** Điểm cuối của hành trình: message rơi đáy thì log và cảnh báo, không im lặng bỏ qua. */
    @DltHandler
    public void handleDlt(ConsumerRecord<String, String> record) {
        log.error("[ORDER-DLT] ☠ {} rơi vào DLT ({}) — nguyên nhân: {}",
                record.value(), record.topic(), exceptionMessage(record));
    }

    /**
     * Chi tiết dễ vấp: hạ tầng của {@code @RetryableTopic} ghi nguyên nhân vào header
     * {@code kafka_exception-message} (hằng số {@link KafkaHeaders#EXCEPTION_MESSAGE}), KHÁC với
     * {@code kafka_dlt-exception-message} mà {@code DeadLetterPublishingRecoverer} dùng khi bạn
     * tự cấu hình (xem luồng payments). Tra cả hai để code không phụ thuộc vào đường đi của message.
     */
    private String exceptionMessage(ConsumerRecord<String, String> record) {
        String message = header(record, KafkaHeaders.EXCEPTION_MESSAGE);
        return message != null ? message : header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
