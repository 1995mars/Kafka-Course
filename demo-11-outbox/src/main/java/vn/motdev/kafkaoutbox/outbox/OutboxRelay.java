package vn.motdev.kafkaoutbox.outbox;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Message Relay — nửa còn lại của Transaction Outbox: quét bảng outbox, đẩy lên Kafka,
 * đánh dấu đã gửi.
 *
 * <p>Guarantee của relay là <b>at-least once</b>: nếu app chết GIỮA lúc "gửi Kafka xong"
 * và "update status=SENT", lần quét sau sẽ gửi lại event đó — thành ra trùng. Vì vậy payload
 * luôn mang eventId để consumer khử trùng ({@code OrderEventsConsumer}). Không mất, chấp
 * nhận trùng, rồi diệt trùng ở consumer — đó là exactly-once thực dụng của slide cuối.
 *
 * <p>Cả batch được gửi trong MỘT Kafka transaction ({@code executeInTransaction}) — consumer
 * read_committed sẽ thấy trọn batch hoặc không thấy gì, không thấy nửa vời.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int MAX_ATTEMPTS = 5;

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public OutboxRelay(OutboxEventRepository outboxRepository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       @Value("${app.topics.order-events}") String topic) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Scheduled(fixedDelay = 2000)
    public void relayPendingEvents() {
        List<OutboxEvent> pending = outboxRepository.findTop20ByStatusOrderByIdAsc(OutboxEvent.Status.PENDING);
        if (pending.isEmpty()) {
            return;
        }
        try {
            kafkaTemplate.executeInTransaction(template -> {
                for (OutboxEvent event : pending) {
                    // send trả CompletableFuture nhưng không cần .get(): commitTransaction
                    // chỉ thành công khi mọi message trong transaction đã được broker ack.
                    template.send(MessageBuilder.withPayload(event.getPayload())
                            .setHeader(KafkaHeaders.TOPIC, topic)
                            .setHeader(KafkaHeaders.KEY, event.getAggregateId())
                            .setHeader("eventId", event.getEventId())
                            .setHeader("eventType", event.getEventType())
                            .build());
                }
                return null;
            });
        } catch (Exception e) {
            // Kafka đang sập? Không sao — event vẫn nằm trong DB, lần quét sau thử lại.
            // Cạn MAX_ATTEMPTS thì chuyển DEAD: dead letter ngay trong DB kèm cảnh báo,
            // đúng như slide "hết retry thì vào dead letter" — nhưng KHÔNG bao giờ mất.
            pending.forEach(ev -> ev.recordFailure(MAX_ATTEMPTS));
            outboxRepository.saveAll(pending);
            long dead = pending.stream().filter(ev -> ev.getStatus() == OutboxEvent.Status.DEAD).count();
            log.warn("[RELAY  ] gửi thất bại ({} event, {} chuyển DEAD): {}",
                    pending.size(), dead, e.getMessage());
            return;
        }

        pending.forEach(OutboxEvent::markSent);
        outboxRepository.saveAll(pending);
        log.info("[RELAY  ] đã publish {} event từ outbox lên '{}'", pending.size(), topic);
    }
}
