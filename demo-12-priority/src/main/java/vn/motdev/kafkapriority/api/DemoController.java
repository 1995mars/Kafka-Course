package vn.motdev.kafkapriority.api;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class DemoController {

    private static final Logger log = LoggerFactory.getLogger(DemoController.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, String> bucketKafkaTemplate;
    private final String highTopic;
    private final String lowTopic;
    private final String reseqInTopic;
    private final String bucketTopic;

    public DemoController(KafkaTemplate<String, String> kafkaTemplate,
                          @Qualifier("bucketKafkaTemplate") KafkaTemplate<String, String> bucketKafkaTemplate,
                          @Value("${app.topics.notify-high}") String highTopic,
                          @Value("${app.topics.notify-low}") String lowTopic,
                          @Value("${app.topics.reseq-in}") String reseqInTopic,
                          @Value("${app.topics.bucket-orders}") String bucketTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.bucketKafkaTemplate = bucketKafkaTemplate;
        this.highTopic = highTopic;
        this.lowTopic = lowTopic;
        this.reseqInTopic = reseqInTopic;
        this.bucketTopic = bucketTopic;
    }

    /** Gửi low TRƯỚC high — log vẫn cho thấy high được xử lý trước: poll-high-first có tác dụng. */
    @PostMapping("/brute/burst")
    public Map<String, String> bruteBurst(@RequestParam(defaultValue = "10") int high,
                                          @RequestParam(defaultValue = "3") int low) {
        for (int i = 1; i <= low; i++) {
            kafkaTemplate.send(lowTopic, "low-" + i, "low-%02d".formatted(i));
        }
        for (int i = 1; i <= high; i++) {
            kafkaTemplate.send(highTopic, "high-" + i, "high-%02d".formatted(i));
        }
        return Map.of("message",
                "đã gửi %d low (trước) + %d high (sau) — xem log [BRUTE]".formatted(low, high));
    }

    /**
     * Phơi bày lỗ hổng starvation: bơm high ĐỀU ĐẶN nhanh hơn tốc độ xử lý (250ms/message
     * đến, 300ms/message xử lý) trong 15 giây — 5 message low gửi từ đầu phải đợi đến khi
     * dòng high ngừng chảy mới được đụng tới.
     */
    @PostMapping("/brute/starve")
    public Map<String, String> bruteStarve() {
        for (int i = 1; i <= 5; i++) {
            kafkaTemplate.send(lowTopic, "low-" + i, "low-%02d (nạn nhân starvation)".formatted(i));
        }
        Thread flooder = new Thread(() -> {
            for (int i = 1; i <= 60; i++) {
                kafkaTemplate.send(highTopic, "high-" + i, "high-%02d (flood)".formatted(i));
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            log.info("[BRUTE  ] flood kết thúc — giờ low mới đến lượt");
        }, "high-flooder");
        flooder.setDaemon(true);
        flooder.start();
        return Map.of("message", "5 low đã vào hàng, high flood chạy 15s — quan sát log [BRUTE]");
    }

    /** Bơm 12 message priority ngẫu nhiên 1–3: 10 cái đầu xả vì buffer đầy, 2 cái cuối chờ timeout 5s. */
    @PostMapping("/resequencer/burst")
    public Map<String, String> resequencerBurst(@RequestParam(defaultValue = "12") int count) {
        for (int i = 1; i <= count; i++) {
            int priority = ThreadLocalRandom.current().nextInt(1, 4);
            kafkaTemplate.send(reseqInTopic, String.valueOf(priority), "order-%02d".formatted(i));
        }
        return Map.of("message",
                "đã gửi %d message priority ngẫu nhiên — so sánh thứ tự [RESEQ] vào vs [RESEQ-OUT] ra"
                        .formatted(count));
    }

    /** Gửi xen kẽ high/low vào CÙNG topic bucket-orders — partitioner tự chia bucket. */
    @PostMapping("/bucket/burst")
    public Map<String, String> bucketBurst(@RequestParam(defaultValue = "12") int high,
                                           @RequestParam(defaultValue = "12") int low) {
        int max = Math.max(high, low);
        for (int i = 1; i <= max; i++) {
            if (i <= high) {
                bucketKafkaTemplate.send(bucketTopic, "high-" + i, "đơn VIP %02d".formatted(i));
            }
            if (i <= low) {
                bucketKafkaTemplate.send(bucketTopic, "low-" + i, "đơn thường %02d".formatted(i));
            }
        }
        return Map.of("message",
                ("đã gửi %d high + %d low vào cùng topic — log [BUCKET]: high 4 thread, "
                        + "low 2 thread, cùng chạy nhưng high drain nhanh gấp đôi").formatted(high, low));
    }
}
