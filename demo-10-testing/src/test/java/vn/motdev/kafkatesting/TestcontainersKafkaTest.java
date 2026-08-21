package vn.motdev.kafkatesting;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Cách 2 — Testcontainers: Kafka THẬT chạy trong Docker, đúng phiên bản bạn dùng ở production.
 * Chậm hơn embedded (phải kéo image, khởi động container) nhưng sát thực tế nhất.
 *
 * <p>Testcontainers cấp phát cổng ĐỘNG nên không bao giờ đụng cổng máy khác — đổi lại địa chỉ
 * broker chỉ biết được lúc chạy. Tài liệu gốc giải quyết bằng {@code @ClassRule} cộng một class
 * {@code @Configuration} tự dựng consumer/producer factory; từ Spring Boot 2.2 có cách gọn hơn
 * nhiều là {@link DynamicPropertySource} — bơm thẳng địa chỉ vào Environment, phần còn lại để
 * autoconfiguration lo.
 *
 * <p>{@code disabledWithoutDocker = true} khiến test tự bỏ qua (skip) thay vì fail trên máy
 * hoặc CI không có Docker.
 */
@SpringBootTest
@DirtiesContext
@Testcontainers(disabledWithoutDocker = true)
class TestcontainersKafkaTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

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
        producer.send(topic, "xin chào Testcontainers");

        boolean received = consumer.await(20);

        assertThat(received)
                .as("consumer phải nhận được message trong 20 giây")
                .isTrue();
        assertThat(consumer.getPayload()).isEqualTo("xin chào Testcontainers");
    }
}
