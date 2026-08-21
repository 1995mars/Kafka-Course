package vn.motdev.kafkatesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Lớp bọc mỏng quanh KafkaTemplate — đúng thứ ta muốn kiểm chứng bằng integration test. */
@Component
public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, String payload) {
        log.info("[PRODUCER] gửi '{}' vào {}", payload, topic);
        kafkaTemplate.send(topic, payload);
    }
}
