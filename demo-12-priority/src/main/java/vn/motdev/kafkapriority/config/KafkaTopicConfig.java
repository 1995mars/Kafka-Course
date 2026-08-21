package vn.motdev.kafkapriority.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
public class KafkaTopicConfig {

    @Bean
    public NewTopic notifyHighTopic(@Value("${app.topics.notify-high}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic notifyLowTopic(@Value("${app.topics.notify-low}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic reseqInTopic(@Value("${app.topics.reseq-in}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic reseqOutTopic(@Value("${app.topics.reseq-out}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }

    /**
     * 6 partition chia làm 2 bucket: 0–3 cho high (4 "nhân công"), 4–5 cho low (2 "nhân công").
     * Tỷ lệ partition chính LÀ tỷ lệ ưu tiên — đó là toàn bộ ý tưởng của Bucket Priority.
     */
    @Bean
    public NewTopic bucketOrdersTopic(@Value("${app.topics.bucket-orders}") String name) {
        return TopicBuilder.name(name).partitions(6).replicas(1).build();
    }
}
