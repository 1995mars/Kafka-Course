package vn.motdev.kafkaoffset.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

/**
 * Mỗi AckMode một container factory, để ba listener trong demo chạy song song mà vẫn
 * so sánh được. Ngoài đời bạn thường chỉ đặt {@code spring.kafka.listener.ack-mode}
 * một lần cho cả ứng dụng.
 *
 * <p>Bảy AckMode của Spring Kafka: RECORD, BATCH (mặc định), TIME, COUNT, COUNT_TIME,
 * MANUAL, MANUAL_IMMEDIATE.
 *
 * <p>{@code ConsumerFactory} ở đây là bean do Spring Boot autoconfiguration dựng từ
 * {@code spring.kafka.consumer.*} — ta chỉ mượn lại chứ không cấu hình lại từ đầu.
 */
@Configuration
public class ListenerFactoryConfig {

    /** Mặc định của Spring: commit MỘT lần sau khi listener xử lý xong toàn bộ batch của poll. */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchAckFactory(
            ConsumerFactory<String, String> consumerFactory) {
        return factory(consumerFactory, AckMode.BATCH);
    }

    /** Commit sau TỪNG bản ghi: ít duplicate hơn hẳn, đổi lại tốn hiệu năng vì commit liên tục. */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> recordAckFactory(
            ConsumerFactory<String, String> consumerFactory) {
        return factory(consumerFactory, AckMode.RECORD);
    }

    /**
     * Bạn tự gọi {@code acknowledgment.acknowledge()}.
     * MANUAL_IMMEDIATE commit ngay tại chỗ; MANUAL gom lại và commit khi hết batch.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualImmediateFactory(
            ConsumerFactory<String, String> consumerFactory) {
        return factory(consumerFactory, AckMode.MANUAL_IMMEDIATE);
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> factory(
            ConsumerFactory<String, String> consumerFactory, AckMode ackMode) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ackMode);
        return factory;
    }
}
