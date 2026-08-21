package vn.motdev.kafkaproducer.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Bản Java config TƯƠNG ĐƯƠNG phần {@code spring.kafka.producer} trong application.yml —
 * dành cho ai thích khai báo bean hơn viết yml.
 *
 * <p>Mặc định KHÔNG chạy (profile "java-config" không được bật). Bật bằng:
 * {@code mvn spring-boot:run -Dspring-boot.run.profiles=local,java-config}.
 * Khi có hai bean này, autoconfiguration của Spring Boot tự lùi lại (@ConditionalOnMissingBean),
 * nên hai cách cấu hình không đá nhau — nhưng cũng đừng khai cả hai rồi thắc mắc cái nào thắng.
 */
@Configuration
@Profile("java-config")
public class ProducerJavaConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(
            ProducerFactory<String, String> producerFactory,
            @Value("${spring.kafka.template.default-topic}") String defaultTopic) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(defaultTopic);
        return template;
    }
}
