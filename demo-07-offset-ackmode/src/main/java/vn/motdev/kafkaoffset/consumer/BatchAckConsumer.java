package vn.motdev.kafkaoffset.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import vn.motdev.kafkaoffset.order.OrderService;

/**
 * AckMode.BATCH — mặc định của Spring Kafka.
 *
 * <p>Offset chỉ được commit SAU KHI listener xử lý xong toàn bộ batch mà poll trả về
 * (ở đây tối đa 5 bản ghi, theo {@code max-poll-records}). Giết tiến trình giữa batch
 * là mọi bản ghi trong batch đó chạy lại từ đầu sau khi restart — duplicate.
 * Business logic phải lường trước điều này (idempotent).
 */
@Component
@Profile("!pitfall & !plain-client")
public class BatchAckConsumer {

    private final OrderService orderService;

    public BatchAckConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.topic}", groupId = "order-batch", containerFactory = "batchAckFactory")
    public void consume(ConsumerRecord<String, String> record) {
        orderService.process("[BATCH  ]", record.value(), record.offset());
    }
}
