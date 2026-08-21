package vn.motdev.kafkaoffset.pitfall;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import vn.motdev.kafkaoffset.order.OrderService;

/**
 * ✅ Bản sửa cho {@link AsyncUnsafeConsumer}: vẫn xử lý bất đồng bộ, nhưng offset chỉ được
 * commit khi công việc THẬT SỰ xong.
 *
 * <p>Ba điều kiện phải đi cùng nhau:
 * <ol>
 *   <li>container dùng AckMode.MANUAL_IMMEDIATE;</li>
 *   <li>truyền {@link Acknowledgment} vào tận thread xử lý;</li>
 *   <li>gọi acknowledge() ở dòng cuối cùng của thread đó.</li>
 * </ol>
 *
 * <p>Đánh đổi: nếu tiến trình chết giữa chừng, message sẽ được xử lý LẠI sau restart —
 * at-least-once. Đó là lựa chọn đúng: thà trùng còn hơn mất, và xử lý trùng thì có thể
 * chống bằng idempotency.
 */
@Component
@Profile("pitfall")
public class AsyncSafeConsumer {

    private final OrderService orderService;
    private final ExecutorService workers = Executors.newFixedThreadPool(4);

    public AsyncSafeConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.topic}", groupId = "order-async-safe", containerFactory = "manualImmediateFactory")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        workers.submit(() -> {
            orderService.process("[SAFE   ]", record.value(), record.offset());
            acknowledgment.acknowledge();
        });
    }

    @PreDestroy
    void shutdown() {
        workers.shutdownNow();
    }
}
