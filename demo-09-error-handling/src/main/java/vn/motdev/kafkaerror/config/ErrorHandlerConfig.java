package vn.motdev.kafkaerror.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Bộ khung xử lý lỗi cho topic payments: {@code DefaultErrorHandler} + backoff +
 * {@code RetryListener} + {@code DeadLetterPublishingRecoverer}.
 *
 * <p>Cố ý KHÔNG khai {@code DefaultErrorHandler} thành bean toàn cục: Spring Boot sẽ tự gắn
 * bean {@code CommonErrorHandler} vào mọi container, kể cả các listener {@code @RetryableTopic}
 * ở {@link vn.motdev.kafkaerror.order.OrderRetryableTopicConsumer} vốn đã có cơ chế riêng.
 * Ở đây error handler chỉ gắn cho đúng factory của payments.
 */
@Configuration
public class ErrorHandlerConfig {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlerConfig.class);

    private final String retryTopic;
    private final String dltTopic;
    private final String backOffType;

    public ErrorHandlerConfig(@Value("${app.topics.payments-retry}") String retryTopic,
                              @Value("${app.topics.payments-dlt}") String dltTopic,
                              @Value("${demo.backoff:exponential}") String backOffType) {
        this.retryTopic = retryTopic;
        this.dltTopic = dltTopic;
        this.backOffType = backOffType;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> paymentListenerFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTemplate<String, String> kafkaTemplate) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(paymentErrorHandler(kafkaTemplate));
        return factory;
    }

    private DefaultErrorHandler paymentErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer(kafkaTemplate), backOff());

        // Nguyên tắc vàng: chỉ retry lỗi TRANSIENT. Dữ liệu sai định dạng thì thử một triệu lần
        // vẫn sai — cho đi thẳng recoverer, khỏi tốn backoff.
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

        // RetryListener: chỗ đẹp nhất để log và đếm metric cho từng lần thử.
        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("[RETRY   ] lần thử {} thất bại cho {} | {}",
                        deliveryAttempt, record.value(), rootCause(ex).getMessage());
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.error("[RECOVER ] hết retry cho {} → chuyển sang topic khác", record.value());
            }
        });
        return errorHandler;
    }

    /**
     * Quyết định message hỏng đi đâu sau khi hết retry:
     * lỗi có thể phục hồi → retry topic (giữ nguyên partition); còn lại → DLT.
     */
    private DeadLetterPublishingRecoverer recoverer(KafkaTemplate<String, String> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, exception) -> {
            String destination = isRecoverable(exception) ? retryTopic : dltTopic;
            log.error("[RECOVER ] {} → {} (do {})",
                    record.value(), destination, rootCause(exception).getClass().getSimpleName());
            return new TopicPartition(destination, record.partition());
        });
    }

    private BackOff backOff() {
        if ("fixed".equalsIgnoreCase(backOffType)) {
            // chờ cố định 1 giây, tối đa 2 lần retry (tổng cộng 3 lượt xử lý)
            return new FixedBackOff(1000L, 2L);
        }
        // Exponential: 1s → 2s (chặn trên maxInterval), tối đa 2 lần retry.
        // Cấu hình thật thường lớn hơn nhiều, và nên cộng thêm jitter để trăm consumer
        // không cùng retry vào một nhịp.
        var backOff = new ExponentialBackOffWithMaxRetries(2);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(2000L);
        return backOff;
    }

    /**
     * Exception mà listener ném ra luôn bị bọc trong ListenerExecutionFailedException, nên
     * phải lần theo chuỗi cause thay vì so sánh trực tiếp kiểu của exception ngoài cùng.
     */
    private boolean isRecoverable(Exception exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (cause instanceof RecoverableDataAccessException) {
                return true;
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return false;
    }

    private Throwable rootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
