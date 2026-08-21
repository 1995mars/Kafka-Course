package vn.motdev.kafkaoffset.plain;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Bốn chiến lược commit của Kafka client THUẦN — không có Spring ở giữa, để thấy chính xác
 * thứ mà AckMode của Spring đang làm hộ bạn.
 *
 * <p>Chạy: {@code mvn spring-boot:run -Dspring-boot.run.profiles=local,plain-client}
 * và đổi {@code demo.commit-strategy} trong application.yml (auto | sync | async | specific).
 *
 * <p>Mỗi chiến lược dùng một group riêng nên chạy lại lần nữa sẽ không đọc lại từ đầu —
 * muốn đọc lại, gửi thêm đơn hàng mới hoặc đổi tên group.
 */
@Component
@Profile("plain-client")
public class PlainClientCommitDemo implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(PlainClientCommitDemo.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration RUN_FOR = Duration.ofSeconds(20);

    private final String bootstrapServers;
    private final String topic;
    private final String strategy;

    public PlainClientCommitDemo(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                 @Value("${app.topic}") String topic,
                                 @Value("${demo.commit-strategy}") String strategy) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.strategy = strategy;
    }

    @Override
    public void run(String... args) {
        log.info("=== Kafka client thuần — chiến lược commit: {} (chạy {} giây) ===",
                strategy, RUN_FOR.toSeconds());
        switch (strategy) {
            case "auto" -> autoCommit();
            case "sync" -> commitSync();
            case "async" -> commitAsync();
            case "specific" -> commitSpecificOffsets();
            default -> throw new IllegalArgumentException(
                    "demo.commit-strategy phải là auto | sync | async | specific, đang là: " + strategy);
        }
        log.info("=== Hết giờ demo. Dừng ứng dụng bằng Ctrl+C ===");
    }

    /**
     * 1) Tự động commit. Cứ mỗi auto.commit.interval.ms, tại vòng poll, consumer commit offset
     * mới nhất của lần poll TRƯỚC. Tiện, nhưng chết giữa hai lần commit là xử lý đúp toàn bộ
     * phần đã làm từ lần commit cuối. Thu nhỏ interval chỉ giảm chứ không loại bỏ được duplicate.
     */
    private void autoCommit() {
        Properties props = baseProps("plain-auto");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = deadline();
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(POLL_TIMEOUT)) {
                    handle("[AUTO    ]", record);
                }
            }
            // close() cũng commit offset lần cuối trước khi rời group — đừng kill -9 nếu tránh được.
        }
    }

    /**
     * 2) commitSync — commit offset mới nhất của lần poll, BLOCK đến khi broker xác nhận và
     * tự retry cho tới khi thành công hoặc gặp lỗi không thể retry. Đáng tin nhất, chậm nhất.
     * Nguyên tắc: xử lý xong hết batch rồi mới commit.
     */
    private void commitSync() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(baseProps("plain-sync"))) {
            consumer.subscribe(List.of(topic));
            long deadline = deadline();
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    handle("[SYNC    ]", record);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    log.info("[SYNC    ] commitSync() xong cho batch {} bản ghi", records.count());
                }
            }
        }
    }

    /**
     * 3) commitAsync — gửi yêu cầu rồi đi tiếp, throughput cao hơn hẳn nhưng KHÔNG tự retry.
     *
     * <p>Vì sao không retry? Giả sử commit offset 2000 hỏng tạm thời, batch sau commit 3000
     * thành công; retry 2000 lúc này sẽ kéo offset LÙI từ 3000 về 2000 — rebalance một phát
     * là cả nghìn message chạy lại. Muốn retry an toàn thì phải gắn sequence number tăng dần
     * vào callback và chỉ retry khi chưa có commit nào mới hơn.
     *
     * <p>Pattern thực dụng: chạy bình thường dùng commitAsync (lần commit sau bù cho lần trước),
     * trước khi đóng gọi một phát commitSync để chốt sổ.
     */
    private void commitAsync() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(baseProps("plain-async"))) {
            consumer.subscribe(List.of(topic));
            long deadline = deadline();
            try {
                while (System.currentTimeMillis() < deadline) {
                    var records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, String> record : records) {
                        handle("[ASYNC   ]", record);
                    }
                    if (!records.isEmpty()) {
                        consumer.commitAsync((offsets, exception) -> {
                            if (exception != null) {
                                log.warn("[ASYNC   ] commit hỏng và sẽ KHÔNG tự retry: {}", exception.getMessage());
                            } else {
                                log.info("[ASYNC   ] commit xong (không chờ): {}", offsets);
                            }
                        });
                    }
                }
            } finally {
                consumer.commitSync(); // chốt sổ trước khi rời group
                log.info("[ASYNC   ] commitSync() lần cuối trước khi đóng");
            }
        }
    }

    /**
     * 4) Tự quản lý map TopicPartition → OffsetAndMetadata: mức kiểm soát cao nhất.
     *
     * <p>Chú ý {@code offset + 1}: offset ta commit là vị trí sẽ ĐỌC TIẾP, nên bằng offset
     * vừa xử lý cộng một. Commit đúng bằng offset vừa xử lý sẽ khiến bản ghi đó chạy lại.
     */
    private void commitSpecificOffsets() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(baseProps("plain-specific"))) {
            consumer.subscribe(List.of(topic));
            Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();
            int processed = 0;
            long deadline = deadline();

            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(POLL_TIMEOUT)) {
                    handle("[SPECIFIC]", record);
                    pending.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));

                    if (++processed % 5 == 0) { // ngoài đời hay dùng mốc 50 hoặc 100
                        consumer.commitSync(pending);
                        log.info("[SPECIFIC] commit sau {} bản ghi: {}", processed, pending);
                    }
                }
            }
            if (!pending.isEmpty()) {
                consumer.commitSync(pending);
                log.info("[SPECIFIC] commit phần dư: {}", pending);
            }
        }
    }

    private Properties baseProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 3 chiến lược manual ghi đè lại
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
        return props;
    }

    private long deadline() {
        return System.currentTimeMillis() + RUN_FOR.toMillis();
    }

    private void handle(String tag, ConsumerRecord<String, String> record) {
        log.info("{} xử lý {} (partition={} offset={})",
                tag, record.value(), record.partition(), record.offset());
    }
}
