package vn.motdev.kafkapriority.bucket;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * Pattern 3 — Bucket Priority: chia partition của CÙNG một topic thành các bucket
 * theo mức ưu tiên. Với 6 partition: bucket HIGH ôm partition 0–3 (4 "nhân công"),
 * bucket LOW ôm partition 4–5 (2 "nhân công").
 *
 * <p>Key của message quyết định bucket: {@code high-*} vào 0–3, còn lại vào 4–5;
 * trong bucket chọn NGẪU NHIÊN cho đều tải (không dùng counter round-robin, vì
 * KafkaProducer có thể gọi {@code partition()} nhiều lần cho một record — counter
 * sẽ nhảy bước và dồn message vào một nửa bucket). So với hash mặc định, ta chỉ
 * thay đổi MỘT điều: key không quyết định partition cụ thể mà quyết định NHÓM partition.
 *
 * <p>Bản production-grade (bucket theo tỷ lệ %, co giãn khi topic đổi số partition,
 * interceptor phía consumer): xem bucket-priority-pattern của Ricardo Ferreira trên GitHub.
 */
public class BucketPriorityPartitioner implements Partitioner {

    static final int HIGH_BUCKET_START = 0;
    static final int HIGH_BUCKET_SIZE = 4;
    static final int LOW_BUCKET_START = 4;
    static final int LOW_BUCKET_SIZE = 2;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        boolean high = key instanceof String s && s.startsWith("high");
        if (high) {
            return HIGH_BUCKET_START + ThreadLocalRandom.current().nextInt(HIGH_BUCKET_SIZE);
        }
        return LOW_BUCKET_START + ThreadLocalRandom.current().nextInt(LOW_BUCKET_SIZE);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
