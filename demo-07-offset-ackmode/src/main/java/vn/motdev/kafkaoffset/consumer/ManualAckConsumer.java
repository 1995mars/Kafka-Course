package vn.motdev.kafkaoffset.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import vn.motdev.kafkaoffset.order.OrderService;

/**
 * AckMode.MANUAL_IMMEDIATE — bạn quyết định thời điểm commit.
 *
 * <p>Chỉ cần thêm tham số {@link Acknowledgment} vào method là Spring tiêm vào.
 * Quy tắc bất di bất dịch: gọi acknowledge() ở DÒNG CUỐI, sau khi đã xử lý thật sự xong.
 * Gọi sớm = tự tay tạo ra kịch bản mất dữ liệu.
 */
@Component
@Profile("!pitfall & !plain-client")
public class ManualAckConsumer {

    private final OrderService orderService;

    public ManualAckConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.topic}", groupId = "order-manual", containerFactory = "manualImmediateFactory")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        orderService.process("[MANUAL ]", record.value(), record.offset());
        acknowledgment.acknowledge();
    }
}
