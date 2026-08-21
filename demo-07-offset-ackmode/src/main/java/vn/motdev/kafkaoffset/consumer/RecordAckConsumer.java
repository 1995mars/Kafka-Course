package vn.motdev.kafkaoffset.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import vn.motdev.kafkaoffset.order.OrderService;

/**
 * AckMode.RECORD — commit ngay sau mỗi bản ghi xử lý xong.
 *
 * <p>Crash chỉ làm chạy lại đúng bản ghi đang dở, không kéo theo cả batch. Cái giá là
 * một lượt commit cho mỗi message: throughput thấp hơn BATCH rõ rệt khi tải cao.
 */
@Component
@Profile("!pitfall & !plain-client")
public class RecordAckConsumer {

    private final OrderService orderService;

    public RecordAckConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.topic}", groupId = "order-record", containerFactory = "recordAckFactory")
    public void consume(ConsumerRecord<String, String> record) {
        orderService.process("[RECORD ]", record.value(), record.offset());
    }
}
