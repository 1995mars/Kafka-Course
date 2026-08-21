package vn.motdev.kafkaproducer;

import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Sinh một số ngẫu nhiên mỗi giây và gửi vào topic mặc định (random-number).
 *
 * <p>KafkaTemplate do Spring Boot autoconfiguration tạo sẵn từ cấu hình trong
 * application.yml — ta chỉ việc inject qua constructor rồi gọi send.
 */
@Component
public class RandomNumberProducer {

    private static final Logger log = LoggerFactory.getLogger(RandomNumberProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public RandomNumberProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 1000)
    public void produce() {
        String value = String.valueOf(ThreadLocalRandom.current().nextInt(1, 101));

        // sendDefault: gửi vào spring.kafka.template.default-topic.
        //
        // Không truyền key → Kafka tự chọn partition bằng partitioner "sticky": nó bám MỘT
        // partition cho tới khi gom đủ batch.size (16KB) rồi mới đổi. Demo bắn 1 message vài
        // byte mỗi giây nên trên thực tế bạn sẽ thấy mọi message vào cùng một partition —
        // đúng hành vi của Kafka, không phải lỗi. Muốn rải đều thì gửi kèm key
        // (send(topic, key, value)) hoặc đổi partitioner (xem demo Buổi 8).
        //
        // Từ Spring Boot 3, send/sendDefault trả về CompletableFuture — non-blocking,
        // ứng dụng không đứng chờ broker xác nhận. whenComplete là chỗ bắt lỗi gửi
        // (KafkaException); chiến lược retry/recovery đầy đủ nằm ở Buổi 9.
        kafkaTemplate.sendDefault(value).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[PRODUCER] gửi thất bại: {}", ex.getMessage());
                return;
            }
            var meta = result.getRecordMetadata();
            log.info("[PRODUCER] gửi {} → topic={} partition={} offset={}",
                    value, meta.topic(), meta.partition(), meta.offset());
        });
    }
}
