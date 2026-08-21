package vn.motdev.kafkaoutbox.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Idempotent consumer — mảnh cuối của "exactly once thực dụng".
 *
 * <p>Relay đảm bảo at-least once, nghĩa là event CÓ THỂ đến hai lần. Consumer khử trùng
 * bằng kỹ thuật số 1 của slide "Exactly Once — khi nào khả thi?": mỗi message mang ID
 * duy nhất, consumer theo dõi các ID đã xử lý (bảng processed_events).
 *
 * <p>Insert eventId + xử lý nghiệp vụ nằm trong CÙNG DB transaction — nếu xử lý ném
 * exception thì cái "đã ghi nhớ" cũng rollback, lần re-deliver sau vẫn được xử lý lại.
 */
@Component
public class OrderEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsConsumer.class);

    private final ProcessedEventRepository processedEventRepository;

    public OrderEventsConsumer(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    // "transactionManager" = TM của JPA — app này có cả kafkaTransactionManager
    // (sinh ra bởi transaction-id-prefix) nên phải gọi đích danh.
    @KafkaListener(topics = "${app.topics.order-events}", groupId = "order-events-consumer")
    @Transactional("transactionManager")
    public void consume(ConsumerRecord<String, String> record,
                        @Header(name = "eventId", required = false) String eventId) {
        if (eventId == null) {
            log.warn("[CONSUME] message không có eventId — không khử trùng được: {}", record.value());
            return;
        }
        if (processedEventRepository.existsById(eventId)) {
            log.info("[CONSUME] ⏭️ BỎ QUA duplicate eventId={} (đã xử lý trước đó)", eventId);
            return;
        }
        try {
            processedEventRepository.save(new ProcessedEvent(eventId));
            processedEventRepository.flush(); // ép unique check chạy NGAY, trước khi xử lý nghiệp vụ
        } catch (DataIntegrityViolationException e) {
            // Instance khác vừa nhanh tay hơn — với chúng ta message này là duplicate.
            log.info("[CONSUME] ⏭️ BỎ QUA duplicate eventId={} (instance khác đã xử lý)", eventId);
            return;
        }
        log.info("[CONSUME] ✅ xử lý eventId={} (partition={} offset={}): {}",
                eventId, record.partition(), record.offset(), record.value());
    }
}
