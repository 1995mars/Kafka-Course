package vn.motdev.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Toàn bộ phía nhận gói trong một method.
 *
 * <p>@KafkaListener là mô hình PUSH đối với code của bạn: Spring giữ vòng lặp poll bên
 * dưới và gọi method mỗi khi có bản ghi mới. group-id lấy từ
 * {@code spring.kafka.consumer.group-id}; muốn ghi đè cho riêng listener này thì thêm
 * thuộc tính {@code groupId} vào annotation.
 *
 * <p>KafkaTemplate cũng có method receive(), nhưng đừng dùng cho luồng chính: mỗi lần gọi
 * nó tạo rồi đóng một Consumer mới và bắt bạn tự biết partition + offset cần đọc.
 */
@Component
public class RandomNumberConsumer {

    private static final Logger log = LoggerFactory.getLogger(RandomNumberConsumer.class);

    @KafkaListener(topics = "random-number")
    public void consume(ConsumerRecord<String, String> record) {
        // Chỉ cần payload thì khai tham số String value là đủ; nhận nguyên ConsumerRecord
        // khi muốn xem cả partition/offset/key — rất hữu ích để quan sát trong lúc học.
        log.info("[CONSUMER] nhận {} ← partition={} offset={}",
                record.value(), record.partition(), record.offset());
    }
}
