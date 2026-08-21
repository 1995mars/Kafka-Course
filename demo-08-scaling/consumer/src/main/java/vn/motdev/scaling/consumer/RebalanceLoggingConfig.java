package vn.motdev.scaling.consumer;

import java.util.Collection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/**
 * In ra partition mà instance này được gán mỗi khi group rebalance — thứ làm cho demo scale
 * "nhìn thấy được". Bean tên {@code kafkaListenerContainerFactory} ghi đè factory mặc định
 * của Spring Boot, nên mọi @KafkaListener trong app đều dùng nó.
 */
@Configuration
public class RebalanceLoggingConfig {

    private static final Logger log = LoggerFactory.getLogger(RebalanceLoggingConfig.class);

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            @Value("${app.instance-id}") String instanceId) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    log.warn("[{}] ⚠ KHÔNG được gán partition nào — số consumer đã vượt số partition, "
                            + "instance này sẽ ngồi không", instanceId);
                } else {
                    log.info("[{}] ✅ được gán {} partition: {}", instanceId, partitions.size(), partitions);
                }
            }

            // Khi listener là ConsumerAwareRebalanceListener, container chỉ gọi các biến thể
            // có tham số Consumer — override onPartitionsRevoked(Collection) sẽ không bao giờ chạy.
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.info("[{}] ↩ bị thu hồi partition: {} (rebalance đang diễn ra)", instanceId, partitions);
            }
        });
        return factory;
    }
}
