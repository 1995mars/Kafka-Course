package vn.motdev.kafkademo.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Producer duy nhất của demo: gửi vị trí tài xế vào topic driver_gps.
 * Key = driverId để mọi message của CÙNG một tài xế vào cùng partition (giữ thứ tự).
 */
@Component
public class DriverLocationProducer {

    private static final Logger log = LoggerFactory.getLogger(DriverLocationProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public DriverLocationProducer(KafkaTemplate<String, String> kafkaTemplate,
                                  @Value("${app.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendLocation(String driverId, double lat, double lng) {
        String message = """
                {"driverId":"%s","lat":%.6f,"lng":%.6f,"ts":%d}"""
                .formatted(driverId, lat, lng, System.currentTimeMillis());

        kafkaTemplate.send(topic, driverId, message).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[PRODUCER] gửi thất bại: {}", ex.getMessage());
            } else {
                var meta = result.getRecordMetadata();
                log.info("[PRODUCER] đã gửi 1 message  →  topic={} partition={} offset={} | {}",
                        meta.topic(), meta.partition(), meta.offset(), message);
            }
        });
    }
}
