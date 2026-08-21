package vn.motdev.scaling.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Producer của demo scaling: tạo topic 3 partition rồi bắn số tuần tự mỗi giây. */
@SpringBootApplication
@EnableScheduling
public class ScalingProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ScalingProducerApplication.class, args);
    }

    @Bean
    public NewTopic sequentialNumberTopic(@Value("${spring.kafka.template.default-topic}") String topic,
                                          @Value("${app.partitions}") int partitions) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(1)
                .build();
    }
}
