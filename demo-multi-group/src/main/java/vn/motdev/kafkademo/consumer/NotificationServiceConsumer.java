package vn.motdev.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer group 2: notification-service — bắn thông báo "tài xế đã xuất phát".
 * Cùng đọc topic driver_gps nhưng KHÔNG tranh message với map-service,
 * vì mỗi group có vị trí đọc (offset) độc lập.
 */
@Component
public class NotificationServiceConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationServiceConsumer.class);

    @KafkaListener(topics = "${app.topic}", groupId = "notification-service")
    public void onDriverLocation(ConsumerRecord<String, String> record) {
        log.info("[NOTIFICATION-SERVICE]  kiểm tra & bắn notification | partition={} offset={} | {}",
                record.partition(), record.offset(), record.value());
    }
}
