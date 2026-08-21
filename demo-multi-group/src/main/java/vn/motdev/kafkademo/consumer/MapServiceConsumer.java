package vn.motdev.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer group 1: map-service — vẽ vị trí tài xế lên bản đồ realtime.
 * groupId riêng → giữ offset riêng → nhận đủ 100% message của topic.
 */
@Component
public class MapServiceConsumer {

    private static final Logger log = LoggerFactory.getLogger(MapServiceConsumer.class);

    @KafkaListener(topics = "${app.topic}", groupId = "map-service")
    public void onDriverLocation(ConsumerRecord<String, String> record) {
        log.info("[MAP-SERVICE]           vẽ vị trí lên bản đồ      | partition={} offset={} | {}",
                record.partition(), record.offset(), record.value());
    }
}
