package vn.motdev.kafkapriority.resequencer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Pattern 2 — Resequencer (Enterprise Integration Patterns): service đứng GIỮA producer
 * và consumer, gom message vào buffer, sắp theo độ ưu tiên rồi publish sang topic đầu ra.
 *
 * <p>Hai trigger xả buffer đúng như slide: <b>đầy</b> (10 message) hoặc <b>hết timeout</b>
 * (5 giây). Bản gốc trong tài liệu dùng Apache Camel với {@code ExpressionResultComparator};
 * ở đây tự viết bằng PriorityQueue + {@code @Scheduled} để thấy rõ ruột gan pattern —
 * đổi sang Camel thì logic không đổi, chỉ đổi vỏ.
 *
 * <p>Điểm trừ phơi ra ngay trong log: message priority 1 đến sớm vẫn phải NẰM CHỜ
 * đến lúc xả buffer mới được đi — độ trễ là cái giá của thứ tự.
 */
@Component
public class ResequencerBuffer {

    private static final Logger log = LoggerFactory.getLogger(ResequencerBuffer.class);
    private static final int MAX_BUFFER = 10;

    /** Sắp theo priority tăng dần (1 = cao nhất); cùng priority thì giữ thứ tự đến (seq). */
    private final PriorityQueue<BufferedMessage> buffer = new PriorityQueue<>(
            Comparator.comparingInt(BufferedMessage::priority)
                    .thenComparingLong(BufferedMessage::seq));
    private final AtomicLong arrivalSeq = new AtomicLong();

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String outTopic;

    public ResequencerBuffer(KafkaTemplate<String, String> kafkaTemplate,
                             @Value("${app.topics.reseq-out}") String outTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.outTopic = outTopic;
    }

    /** Key của message = priority ("1".."9"), value = nội dung. */
    @KafkaListener(topics = "${app.topics.reseq-in}", groupId = "resequencer")
    public void buffer(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
        int priority = Integer.parseInt(record.key());
        List<BufferedMessage> toFlush = null;
        synchronized (buffer) {
            buffer.add(new BufferedMessage(priority, arrivalSeq.incrementAndGet(), record.value()));
            log.info("[RESEQ  ] vào buffer p={} '{}' (buffer={}/{})",
                    priority, record.value(), buffer.size(), MAX_BUFFER);
            if (buffer.size() >= MAX_BUFFER) {
                toFlush = drain();
            }
        }
        if (toFlush != null) {
            publish(toFlush, "buffer đầy");
        }
    }

    @Scheduled(fixedDelay = 5000)
    public void flushOnTimeout() {
        List<BufferedMessage> toFlush;
        synchronized (buffer) {
            if (buffer.isEmpty()) {
                return;
            }
            toFlush = drain();
        }
        publish(toFlush, "hết timeout 5s");
    }

    /** PriorityQueue chỉ đảm bảo phần tử ĐẦU nhỏ nhất — muốn cả danh sách có thứ tự phải poll dần. */
    private List<BufferedMessage> drain() {
        List<BufferedMessage> sorted = new ArrayList<>(buffer.size());
        while (!buffer.isEmpty()) {
            sorted.add(buffer.poll());
        }
        return sorted;
    }

    private void publish(List<BufferedMessage> messages, String reason) {
        log.info("[RESEQ  ] ▶ xả {} message ({}) — theo thứ tự ưu tiên:", messages.size(), reason);
        for (BufferedMessage msg : messages) {
            kafkaTemplate.send(outTopic, String.valueOf(msg.priority()), msg.value());
        }
    }

    private record BufferedMessage(int priority, long seq, String value) {
    }
}
