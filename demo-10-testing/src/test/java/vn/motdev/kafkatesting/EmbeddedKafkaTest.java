package vn.motdev.kafkatesting;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Cách 1 — Embedded Kafka: broker in-memory do spring-kafka-test dựng ngay trong JVM của test.
 * Nhanh, không cần Docker, hợp cho vòng lặp dev hằng ngày và CI không có Docker.
 *
 * <p>Ba mảnh ghép:
 * <ul>
 *   <li>{@code @SpringBootTest} dựng context thật;</li>
 *   <li>{@code @DirtiesContext} vứt context sau class, tránh broker của test này lẫn sang test khác;</li>
 *   <li>{@code @EmbeddedKafka} tạo broker và topic.</li>
 * </ul>
 *
 * <p>Khác tài liệu gốc một chỗ có chủ đích: không ghim cổng 9092 bằng {@code brokerProperties}.
 * Cổng cố định sẽ đụng ngay khi máy đang chạy Kafka bằng Docker cho các buổi khác. Thay vào đó
 * lấy địa chỉ broker mà EmbeddedKafka tự chọn qua {@code ${spring.embedded.kafka.brokers}}.
 *
 * <p>Nhược điểm duy nhất: đây không phải Kafka "thật" trăm phần trăm, đôi khi hành vi lệch nhẹ
 * so với bản production.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = "test-topic")
class EmbeddedKafkaTest {

    @Autowired
    private MessageProducer producer;

    @Autowired
    private MessageConsumer consumer;

    @Value("${app.topic}")
    private String topic;

    @BeforeEach
    void resetLatch() {
        consumer.reset();
    }

    @Test
    void message_gui_di_phai_toi_duoc_consumer() throws InterruptedException {
        producer.send(topic, "xin chào Kafka");

        boolean received = consumer.await(10);

        assertThat(received)
                .as("consumer phải nhận được message trong 10 giây")
                .isTrue();
        assertThat(consumer.getPayload()).isEqualTo("xin chào Kafka");
    }
}
