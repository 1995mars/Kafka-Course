package vn.motdev.kafkapriority.bucket;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * KafkaTemplate RIÊNG cho demo bucket: khác template mặc định đúng một dòng —
 * {@code partitioner.class} trỏ vào {@link BucketPriorityPartitioner}.
 *
 * <p>Để template riêng thay vì gắn partitioner vào producer chung của app, vì các demo
 * khác (brute-force, resequencer) cần hành vi partition mặc định.
 */
@Configuration
public class BucketProducerConfig {

    /**
     * Cạm bẫy đã dính thật khi làm demo này: chỉ cần TỒN TẠI một bean KafkaTemplate
     * (bucketKafkaTemplate) là auto-configuration của Boot rút lui — template mặc định
     * biến mất, và mọi chỗ inject "kafkaTemplate" lặng lẽ nhận nhầm template có
     * bucket-partitioner, gửi vào partition không tồn tại của các topic 1 partition.
     * Vì vậy phải tự khai lại template mặc định và đánh dấu {@code @Primary}
     * (vẫn dùng ProducerFactory do Boot dựng từ application.yml).
     */
    @Bean
    @Primary
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public KafkaTemplate<String, String> bucketKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.PARTITIONER_CLASS_CONFIG, BucketPriorityPartitioner.class)));
    }
}
