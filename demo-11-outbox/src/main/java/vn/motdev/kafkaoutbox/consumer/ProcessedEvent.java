package vn.motdev.kafkaoutbox.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Sổ ghi nhớ "eventId nào đã xử lý rồi" của idempotent consumer.
 *
 * <p>eventId là khóa chính — hai instance consumer có cùng lúc xử lý một event thì
 * chỉ một bên insert thành công, bên kia dính unique violation và biết là trùng.
 * Production nên dọn định kỳ các dòng cũ (event trùng chỉ đến trong khoảng thời gian ngắn).
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(length = 36)
    private String eventId;

    private Instant processedAt = Instant.now();

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId) {
        this.eventId = eventId;
    }

    public String getEventId() {
        return eventId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
