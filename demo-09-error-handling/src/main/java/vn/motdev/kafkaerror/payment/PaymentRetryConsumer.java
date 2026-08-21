package vn.motdev.kafkaerror.payment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Hướng recovery thứ nhất — QUYẾT TÂM XỬ LÝ LẠI.
 *
 * <p>Message bị đẩy sang retry topic được một listener riêng (group riêng) xử lý lại bằng
 * đúng service ban đầu. Nhờ chạy ở topic khác, những message "khó" này không còn chặn luồng
 * chính của topic payments.
 *
 * <p>Ở đây bắt exception tại chỗ thay vì để ném ra ngoài: nếu vẫn hỏng, đẩy tiếp sang DLT.
 * Để nó ném ra ngoài với một error handler cũng trỏ về retry topic là tự tạo vòng lặp vô tận.
 */
@Component
public class PaymentRetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentRetryConsumer.class);

    private final PaymentService paymentService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String dltTopic;

    public PaymentRetryConsumer(PaymentService paymentService,
                                KafkaTemplate<String, String> kafkaTemplate,
                                @Value("${app.topics.payments-dlt}") String dltTopic) {
        this.paymentService = paymentService;
        this.kafkaTemplate = kafkaTemplate;
        this.dltTopic = dltTopic;
    }

    @KafkaListener(topics = "${app.topics.payments-retry}", groupId = "payment-retry-service")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("[RETRY-T ] xử lý lại {}", record.value());
        try {
            paymentService.process(record.value());
            log.info("[RETRY-T ] 🎉 cứu được {} ở retry topic", record.value());
        } catch (RuntimeException ex) {
            log.error("[RETRY-T ] vẫn hỏng: {} → đẩy sang {}", ex.getMessage(), dltTopic);
            kafkaTemplate.send(dltTopic, record.key(), record.value());
        }
    }
}
