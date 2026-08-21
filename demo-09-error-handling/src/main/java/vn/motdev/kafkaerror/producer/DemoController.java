package vn.motdev.kafkaerror.producer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Bấm nút gây lỗi:
 *
 * <pre>
 * curl -X POST "http://localhost:8080/api/payments?type=flaky"        # retry cứu được
 * curl -X POST "http://localhost:8080/api/payments?type=recoverable"  # hết retry → retry topic
 * curl -X POST "http://localhost:8080/api/payments?type=invalid"      # không retry → DLT
 * curl -X POST "http://localhost:8080/api/orders?type=fail"           # @RetryableTopic → DLT
 * curl -X POST "http://localhost:8080/api/producer-failure"           # lỗi phía producer
 * </pre>
 */
@RestController
public class DemoController {

    private final ResilientEventPublisher publisher;
    private final String paymentsTopic;
    private final String ordersTopic;
    private final AtomicInteger counter = new AtomicInteger();

    public DemoController(ResilientEventPublisher publisher,
                          @Value("${app.topics.payments}") String paymentsTopic,
                          @Value("${app.topics.orders}") String ordersTopic) {
        this.publisher = publisher;
        this.paymentsTopic = paymentsTopic;
        this.ordersTopic = ordersTopic;
    }

    @PostMapping("/api/payments")
    public Map<String, String> payment(@RequestParam(defaultValue = "ok") String type) {
        return publish(paymentsTopic, type);
    }

    @PostMapping("/api/orders")
    public Map<String, String> order(@RequestParam(defaultValue = "ok") String type) {
        return publish(ordersTopic, type);
    }

    /**
     * Gửi vào topic không tồn tại (broker đã tắt auto-create) → producer kiên trì tới
     * delivery.timeout.ms rồi ném TimeoutException. Xem log sau ~8 giây.
     */
    @PostMapping("/api/producer-failure")
    public Map<String, String> producerFailure() {
        return publish("topic-khong-ton-tai", "ok");
    }

    private Map<String, String> publish(String topic, String type) {
        String payload = "%s-%04d".formatted(type, counter.incrementAndGet());
        publisher.publish(topic, payload, payload);
        return Map.of("topic", topic, "payload", payload, "hint", "theo dõi log ứng dụng");
    }
}
