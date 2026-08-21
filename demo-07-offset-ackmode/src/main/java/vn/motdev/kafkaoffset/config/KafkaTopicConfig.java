package vn.motdev.kafkaoffset.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

/** Tạo topic orders lúc khởi động — chỉ ở môi trường dev (profile local). */
@Configuration
@Profile("local")
public class KafkaTopicConfig {

    @Bean
    public NewTopic ordersTopic(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(1) // 1 partition để thứ tự đơn hàng in ra đúng thứ tự gửi, dễ đối chiếu
                .replicas(1)
                .build();
    }
}
