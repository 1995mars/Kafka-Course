package vn.motdev.kafkaoutbox.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Một dòng trong bảng outbox = một message ĐANG CHỜ lên Kafka.
 *
 * <p>Điểm mấu chốt của pattern: dòng này được insert trong CÙNG database transaction
 * với dữ liệu nghiệp vụ. Kafka có sập, app có bị kill — message vẫn nằm an toàn trong DB,
 * relay sẽ gửi lại khi mọi thứ sống dậy. Đây chính là "đã ghi xuống chỗ bền vững trước
 * khi lên Kafka" mà idempotent producer đơn thuần không có được với nguồn request-driven.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    public enum Status {
        PENDING,  // chờ relay gửi
        SENT,     // đã lên Kafka thành công
        DEAD      // cạn retry — dead letter ngay trong DB, chờ người xử lý tay
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** ID duy nhất của event — consumer dùng chính ID này để khử trùng lặp. */
    @Column(nullable = false, unique = true, length = 36)
    private String eventId = UUID.randomUUID().toString();

    /** Kafka key = aggregateId (orderId) để mọi event của một đơn vào cùng partition. */
    @Column(nullable = false)
    private String aggregateId;

    private String eventType;

    @Lob
    @Column(nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status = Status.PENDING;

    private int attempts;
    private Instant createdAt = Instant.now();
    private Instant sentAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String aggregateId, String eventType, String payload) {
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
    }

    public void markSent() {
        this.status = Status.SENT;
        this.sentAt = Instant.now();
    }

    public void recordFailure(int maxAttempts) {
        this.attempts++;
        if (this.attempts >= maxAttempts) {
            this.status = Status.DEAD;
        }
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public Status getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getSentAt() {
        return sentAt;
    }
}
