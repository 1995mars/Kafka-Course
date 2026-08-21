package vn.motdev.kafkapriority.bucket;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * Hai "đội nhân công" của Bucket Priority — gán partition TĨNH, không dùng group
 * rebalancing, vì bucket nào ôm partition nào là quyết định thiết kế, không để
 * Kafka tự chia.
 *
 * <p>Đội HIGH: 4 partition, concurrency 4 → 4 thread xử lý song song.
 * Đội LOW: 2 partition, concurrency 2. Cùng tốc độ xử lý một message (500ms),
 * đội HIGH thoát hàng nhanh gấp đôi — ưu tiên bằng PHÂN BỔ TÀI NGUYÊN, không chen hàng,
 * và không ai bị bỏ đói như brute-force.
 */
@Component
public class BucketConsumers {

    private static final Logger log = LoggerFactory.getLogger(BucketConsumers.class);

    @KafkaListener(id = "bucket-high", groupId = "bucket-workers",
            concurrency = "4",
            topicPartitions = @TopicPartition(topic = "${app.topics.bucket-orders}",
                    partitions = {"0", "1", "2", "3"}))
    public void consumeHigh(ConsumerRecord<String, String> record) {
        process(record, "HIGH");
    }

    @KafkaListener(id = "bucket-low", groupId = "bucket-workers",
            concurrency = "2",
            topicPartitions = @TopicPartition(topic = "${app.topics.bucket-orders}",
                    partitions = {"4", "5"}))
    public void consumeLow(ConsumerRecord<String, String> record) {
        process(record, "LOW ");
    }

    private void process(ConsumerRecord<String, String> record, String bucket) {
        log.info("[BUCKET ] [{}] {} (partition={} thread={})",
                bucket, record.value(), record.partition(), Thread.currentThread().getName());
        try {
            Thread.sleep(500); // giả lập xử lý — để thấy high drain nhanh hơn nhờ nhiều thread
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
