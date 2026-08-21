package vn.motdev.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Tạo topic driver_gps lúc khởi động (qua KafkaAdmin autoconfig của Spring Boot).
 * 3 partition — nhưng số partition KHÔNG liên quan đến việc bao nhiêu group nhận message:
 * mỗi consumer group luôn nhận đủ 100% message của topic.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic driverGpsTopic(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
