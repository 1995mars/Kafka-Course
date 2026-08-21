package vn.motdev.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer group 3: working-hours-service — tính giờ làm việc của tài xế.
 */
@Component
public class WorkingHoursConsumer {

    private static final Logger log = LoggerFactory.getLogger(WorkingHoursConsumer.class);

    @KafkaListener(topics = "${app.topic}", groupId = "working-hours-service")
    public void onDriverLocation(ConsumerRecord<String, String> record) {
        log.info("[WORKING-HOURS-SERVICE] cộng dồn giờ làm việc     | partition={} offset={} | {}",
                record.partition(), record.offset(), record.value());
    }
}
