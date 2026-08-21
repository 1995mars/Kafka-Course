package vn.motdev.kafkaoffset.order;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Bắn một loạt đơn hàng có số thứ tự để dễ nhận ra message nào bị xử lý lại. */
@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;
    private final AtomicInteger counter = new AtomicInteger();

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate,
                         @Value("${app.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public List<String> send(int count) {
        List<String> sent = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String order = "ORD-%04d".formatted(counter.incrementAndGet());
            kafkaTemplate.send(topic, order, order);
            sent.add(order);
        }
        log.info("[PRODUCER] đã gửi {} đơn hàng: {} … {}", count, sent.getFirst(), sent.getLast());
        return sent;
    }
}
