package vn.motdev.kafkaerror.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topic của demo. Ba topic của luồng payments phải khai báo tay; các topic retry của
 * {@code @RetryableTopic} (orders-retry-0, orders-retry-1, …, orders-dlt) do Spring tự tạo
 * nhờ {@code autoCreateTopics = "true"}.
 *
 * <p>Retry topic và DLT để CÙNG số partition với topic gốc, vì recoverer giữ nguyên chỉ số
 * partition khi chuyển message qua.
 */
@Configuration
@Profile("local")
public class KafkaTopicConfig {

    @Bean
    public NewTopic paymentsTopic(@Value("${app.topics.payments}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsRetryTopic(@Value("${app.topics.payments-retry}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsDltTopic(@Value("${app.topics.payments-dlt}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic ordersTopic(@Value("${app.topics.orders}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }
}
