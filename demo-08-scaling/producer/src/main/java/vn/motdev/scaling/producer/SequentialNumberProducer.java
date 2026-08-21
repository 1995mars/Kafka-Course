package vn.motdev.scaling.producer;

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Bắn 1, 2, 3... mỗi giây một số.
 *
 * <p>Số TUẦN TỰ (thay vì ngẫu nhiên như Buổi 6) để bạn nhìn một cái là biết consumer đang
 * tụt lại bao xa: producer in tới 36 mà consumer mới ở 15 nghĩa là đang tồn đọng 21 message.
 *
 * <p>Không gửi kèm key, và producer được cấu hình dùng RoundRobinPartitioner (xem
 * application.yml) nên mỗi message đi một partition — nhờ đó thêm consumer là chia được việc
 * ngay. Đừng bỏ cấu hình đó: partitioner mặc định sẽ dồn cả demo vào một partition.
 */
@Component
public class SequentialNumberProducer {

    private static final Logger log = LoggerFactory.getLogger(SequentialNumberProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AtomicInteger counter = new AtomicInteger();

    public SequentialNumberProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedRate = 1000)
    public void produce() {
        String value = String.valueOf(counter.incrementAndGet());
        kafkaTemplate.sendDefault(value).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[PRODUCER] gửi {} thất bại: {}", value, ex.getMessage());
                return;
            }
            log.info("[PRODUCER] gửi {} → partition={}", value, result.getRecordMetadata().partition());
        });
    }
}
