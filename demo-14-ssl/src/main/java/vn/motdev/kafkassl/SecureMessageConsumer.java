package vn.motdev.kafkassl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SecureMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(SecureMessageConsumer.class);

    @KafkaListener(topics = "${app.topics.secure-messages}", groupId = "ssl-demo")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("[SSL    ] 🔒 nhận (partition={} offset={}): {}",
                record.partition(), record.offset(), record.value());
    }
}
