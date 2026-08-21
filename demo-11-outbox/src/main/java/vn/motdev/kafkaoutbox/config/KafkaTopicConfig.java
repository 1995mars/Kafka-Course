package vn.motdev.kafkaoutbox.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Một topic duy nhất: order-events. Key là orderId nên mọi event của cùng một đơn hàng
 * rơi vào cùng partition — thứ tự "OrderPlaced trước, OrderCancelled sau" được bảo toàn.
 */
@Configuration
@Profile("local")
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderEventsTopic(@Value("${app.topics.order-events}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }
}
