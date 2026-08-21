package vn.motdev.kafkaerror.payment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listener của topic payments — bản thân nó KHÔNG có dòng try/catch nào.
 *
 * <p>Toàn bộ retry và recovery nằm ở {@code paymentListenerFactory}: listener chỉ cần ném
 * exception ra ngoài, {@code DefaultErrorHandler} lo phần còn lại. Đây là cách chia việc
 * đúng: business logic nói "tôi hỏng", hạ tầng quyết định "hỏng thì làm gì".
 *
 * <p>Lưu ý retry ở đây là BLOCKING: trong lúc bản ghi này chờ backoff, partition của nó
 * đứng im. Muốn không tắc luồng chính thì xem {@code @RetryableTopic} ở demo orders.
 */
@Component
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private final PaymentService paymentService;

    public PaymentConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "${app.topics.payments}", groupId = "payment-service",
            containerFactory = "paymentListenerFactory")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("[PAYMENT ] nhận {} (partition={} offset={})",
                record.value(), record.partition(), record.offset());
        paymentService.process(record.value());
    }
}
