package vn.motdev.kafkaoffset.order;

import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Bơm đơn hàng vào topic:
 *
 * <pre>curl -X POST "http://localhost:8080/api/orders?count=10"</pre>
 */
@RestController
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/api/orders")
    public Map<String, Object> publish(@RequestParam(defaultValue = "10") int count) {
        List<String> sent = producer.send(count);
        return Map.of("published", sent.size(), "orders", sent);
    }
}
