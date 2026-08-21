package vn.motdev.kafkassl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Demo Buổi 14 — Kafka Security với SSL.
 *
 * <p>Code producer/consumer KHÔNG khác gì các demo trước — toàn bộ "áo giáp" nằm ở config:
 * <ul>
 *   <li>Broker: listener SSL + keystore (private key + cert được CA ký) — docker-compose.yml.</li>
 *   <li>Client: {@code security.protocol=SSL} + truststore chứa CA — application.yml.</li>
 * </ul>
 *
 * <p>SSL handshake (bất đối xứng, trao Session Key) rồi truyền dữ liệu (đối xứng) diễn ra
 * hoàn toàn bên dưới Kafka client — ứng dụng không phải viết một dòng mã hóa nào.
 */
@SpringBootApplication
public class KafkaSslDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSslDemoApplication.class, args);
    }
}
