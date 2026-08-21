package vn.motdev.kafkapriority.resequencer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer cuối luồng resequencer — chỉ việc đọc reseq-out theo thứ tự partition
 * (1 partition nên FIFO tuyệt đối) và in ra: message giờ đã xếp hàng theo priority.
 */
@Component
public class ResequencedConsumer {

    private static final Logger log = LoggerFactory.getLogger(ResequencedConsumer.class);

    @KafkaListener(topics = "${app.topics.reseq-out}", groupId = "reseq-out-consumer")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("[RESEQ-OUT] p={} '{}'", record.key(), record.value());
    }
}
