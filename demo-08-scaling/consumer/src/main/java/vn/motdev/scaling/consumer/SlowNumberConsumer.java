package vn.motdev.scaling.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer cố tình chậm: producer bắn 1 message/giây, con này mất 2 giây mới xử lý xong một
 * message. Một instance thì không bao giờ đuổi kịp — khoảng cách doãng ra vô hạn.
 *
 * <p>Cách chữa không phải là máy to hơn mà là scale OUT: thêm instance vào cùng consumer group.
 * Trần của việc đó là SỐ PARTITION — instance thứ 4 trên topic 3 partition sẽ ngồi chơi.
 */
@Component
public class SlowNumberConsumer {

    private static final Logger log = LoggerFactory.getLogger(SlowNumberConsumer.class);

    private final long processingTime;
    private final String instanceId;

    public SlowNumberConsumer(@Value("${app.message-processing-time}") long processingTime,
                              @Value("${app.instance-id}") String instanceId) {
        this.processingTime = processingTime;
        this.instanceId = instanceId;
    }

    @KafkaListener(topics = "${app.topic}")
    public void consume(ConsumerRecord<String, String> record) throws InterruptedException {
        Thread.sleep(processingTime); // giả lập gọi API/ghi DB chậm
        log.info("[{}] xử lý xong {} | partition={} offset={}",
                instanceId, record.value(), record.partition(), record.offset());
    }
}
