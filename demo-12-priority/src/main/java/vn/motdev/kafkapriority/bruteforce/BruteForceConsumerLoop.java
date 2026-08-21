package vn.motdev.kafkapriority.bruteforce;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Pattern 1 — Brute-force: mỗi mức ưu tiên một topic, poll topic cao TRƯỚC,
 * sạch message rồi mới ngó xuống topic thấp.
 *
 * <p>Không dùng {@code @KafkaListener} được vì listener không cho kiểm soát THỨ TỰ poll
 * giữa hai topic — phải tự cầm {@link KafkaConsumer} và viết vòng lặp. Đây cũng là dịp
 * thấy cái giá của pattern: code hạ tầng tự viết, tự commit, tự quản lifecycle.
 *
 * <p>Lỗ hổng cố tình phơi ra: chừng nào notify-high còn message thì {@code continue}
 * quay về poll high ngay — notify-low <b>starvation</b>. Bấm {@code /api/brute/starve} để xem.
 */
@Component
public class BruteForceConsumerLoop implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BruteForceConsumerLoop.class);

    private final String bootstrapServers;
    private final String highTopic;
    private final String lowTopic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile KafkaConsumer<String, String> highConsumer;
    private volatile KafkaConsumer<String, String> lowConsumer;
    private Thread worker;

    public BruteForceConsumerLoop(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                  @Value("${app.topics.notify-high}") String highTopic,
                                  @Value("${app.topics.notify-low}") String lowTopic) {
        this.bootstrapServers = bootstrapServers;
        this.highTopic = highTopic;
        this.lowTopic = lowTopic;
    }

    @Override
    public void start() {
        running.set(true);
        worker = new Thread(this::pollLoop, "brute-force-loop");
        worker.start();
    }

    private void pollLoop() {
        highConsumer = newConsumer("brute-force-high");
        lowConsumer = newConsumer("brute-force-low");
        highConsumer.subscribe(List.of(highTopic));
        lowConsumer.subscribe(List.of(lowTopic));
        try {
            while (running.get()) {
                ConsumerRecords<String, String> high = highConsumer.poll(Duration.ofMillis(300));
                if (!high.isEmpty()) {
                    process(high, "HIGH");
                    highConsumer.commitSync();
                    continue; // còn hàng ưu tiên cao → quay lại poll high NGAY, low phải chờ
                }
                ConsumerRecords<String, String> low = lowConsumer.poll(Duration.ofMillis(300));
                if (!low.isEmpty()) {
                    process(low, "LOW ");
                    lowConsumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // stop() gọi wakeup() — thoát vòng lặp êm đẹp
        } finally {
            highConsumer.close();
            lowConsumer.close();
        }
    }

    private void process(ConsumerRecords<String, String> records, String level) {
        for (ConsumerRecord<String, String> record : records) {
            log.info("[BRUTE  ] xử lý [{}] {}", level, record.value());
            try {
                Thread.sleep(300); // giả lập mỗi message tốn 300ms — để starvation nhìn thấy được
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private KafkaConsumer<String, String> newConsumer(String groupId) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                // mỗi lượt poll lấy ít thôi để nhịp "high chen ngang low" hiện rõ trong log
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5));
    }

    @Override
    public void stop() {
        running.set(false);
        if (highConsumer != null) {
            highConsumer.wakeup();
        }
        if (lowConsumer != null) {
            lowConsumer.wakeup();
        }
        if (worker != null) {
            try {
                worker.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
