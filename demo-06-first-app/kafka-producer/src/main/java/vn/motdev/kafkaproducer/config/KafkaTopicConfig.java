package vn.motdev.kafkaproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Tạo topic bằng code lúc khởi động: Spring Boot tự cấu hình bean KafkaAdmin, và
 * KafkaAdmin tạo mọi bean {@link NewTopic} tìm thấy trong context.
 *
 * <p>@Profile("local") theo đúng khuyến nghị của bài: chỉ để máy dev tự tạo topic.
 * Trên production hãy tạo topic chủ động (script/IaC) — nếu để broker auto-create,
 * topic sinh ra chỉ có 1 partition, thứ gần như không bao giờ là điều bạn muốn.
 */
@Configuration
@Profile("local")
public class KafkaTopicConfig {

    @Bean
    public NewTopic randomNumberTopic(@Value("${spring.kafka.template.default-topic}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                // cluster demo chỉ có 1 broker; production dùng 3 để chịu được lỗi node
                .replicas(1)
                .build();
    }
}
