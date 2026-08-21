package vn.motdev.kafkaoffset.pitfall;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import vn.motdev.kafkaoffset.order.OrderService;

/**
 * ❌ CẠM BẪY NGUY HIỂM NHẤT CỦA BUỔI 7 — đây là ví dụ về cách làm SAI.
 *
 * <p>Listener đẩy việc sang thread pool rồi return ngay. Với AckMode.BATCH, Spring thấy
 * listener "đã xong" nên commit offset — trong khi thread pool còn đang xử lý. Ứng dụng
 * chết trong khoảng thời gian đó thì message MẤT VĨNH VIỄN: offset đã commit rồi, instance
 * mới sẽ không bao giờ đọc lại.
 *
 * <p>Nguy hiểm ở chỗ nó chạy rất nhanh và trông rất "ngon" trên môi trường dev.
 *
 * @see AsyncSafeConsumer bản sửa đúng
 */
@Component
@Profile("pitfall")
public class AsyncUnsafeConsumer {

    private static final Logger log = LoggerFactory.getLogger(AsyncUnsafeConsumer.class);

    private final OrderService orderService;
    private final ExecutorService workers = Executors.newFixedThreadPool(4);

    public AsyncUnsafeConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.topic}", groupId = "order-async-unsafe", containerFactory = "batchAckFactory")
    public void consume(ConsumerRecord<String, String> record) {
        workers.submit(() -> orderService.process("[UNSAFE ]", record.value(), record.offset()));
        // return NGAY tại đây → Spring coi như xử lý xong → commit offset.
        log.warn("[UNSAFE ] đã giao {} cho thread pool và return — offset sắp được commit dù chưa xử lý xong!",
                record.value());
    }

    @PreDestroy
    void shutdown() {
        workers.shutdownNow();
    }
}
