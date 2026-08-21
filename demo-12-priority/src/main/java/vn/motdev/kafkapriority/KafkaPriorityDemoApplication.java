package vn.motdev.kafkapriority;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Demo Buổi 12 — Kafka không có message priority, và 3 cách "lách":
 *
 * <ol>
 *   <li><b>Brute-force</b> (package {@code bruteforce}) — topic riêng cho từng mức,
 *       poll topic cao trước. Đơn giản nhưng dễ starvation.</li>
 *   <li><b>Resequencer</b> (package {@code resequencer}) — buffer + sắp xếp lại giữa
 *       hai topic. Có thứ tự nhưng message VIP vẫn phải nằm chờ.</li>
 *   <li><b>Bucket Priority</b> (package {@code bucket}) — chia partition của CÙNG một topic
 *       thành bucket, bucket cao nhiều partition (nhiều consumer) hơn. Không ai bị bỏ đói.</li>
 * </ol>
 *
 * <p>{@code @EnableScheduling} phục vụ nhịp xả buffer của resequencer.
 */
@SpringBootApplication
@EnableScheduling
public class KafkaPriorityDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaPriorityDemoApplication.class, args);
    }
}
