package vn.motdev.kafkassl.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * AdminClient tạo topic cũng đi qua SSL — cùng bootstrap-servers và truststore
 * với producer/consumer, Spring Boot tự lo.
 */
@Configuration
@Profile("local")
public class KafkaTopicConfig {

    @Bean
    public NewTopic secureMessagesTopic(@Value("${app.topics.secure-messages}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }
}
