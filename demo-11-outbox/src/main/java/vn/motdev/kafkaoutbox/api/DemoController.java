package vn.motdev.kafkaoutbox.api;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vn.motdev.kafkaoutbox.order.OrderService;
import vn.motdev.kafkaoutbox.order.PurchaseOrder;
import vn.motdev.kafkaoutbox.order.PurchaseOrderRepository;
import vn.motdev.kafkaoutbox.outbox.OutboxEventRepository;

@RestController
@RequestMapping("/api")
public class DemoController {

    private final OrderService orderService;
    private final PurchaseOrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public DemoController(OrderService orderService,
                          PurchaseOrderRepository orderRepository,
                          OutboxEventRepository outboxRepository,
                          KafkaTemplate<String, String> kafkaTemplate,
                          @Value("${app.topics.order-events}") String topic) {
        this.orderService = orderService;
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /** Luồng chuẩn: 1 transaction ghi đơn hàng + outbox → relay publish → consumer xử lý. */
    @PostMapping("/orders")
    public ResponseEntity<?> placeOrder(@RequestParam(defaultValue = "laptop") String product,
                                        @RequestParam(defaultValue = "1") int quantity,
                                        @RequestParam(defaultValue = "false") boolean fail) {
        try {
            PurchaseOrder order = orderService.placeOrder(product, quantity, fail);
            return ResponseEntity.ok(Map.of(
                    "orderId", order.getId(),
                    "message", "đơn + outbox event đã commit — chờ relay (tối đa 2s) đẩy lên Kafka"));
        } catch (IllegalStateException e) {
            return ResponseEntity.internalServerError().body(Map.of(
                    "error", e.getMessage(),
                    "orders", orderRepository.count(),
                    "outbox", outboxRepository.count(),
                    "message", "rollback CẢ HAI bảng — Kafka không nhận gì, xem 2 count ở trên"));
        }
    }

    /** Nhìn vào bảng outbox: PENDING đang chờ, SENT đã đi, DEAD cạn retry. */
    @GetMapping("/outbox")
    public List<Map<String, Object>> outbox() {
        return outboxRepository.findAll().stream()
                .<Map<String, Object>>map(ev -> Map.of(
                        "id", ev.getId(),
                        "eventId", ev.getEventId(),
                        "aggregateId", ev.getAggregateId(),
                        "status", ev.getStatus().name(),
                        "attempts", ev.getAttempts()))
                .toList();
    }

    /**
     * Giả lập chính xác tình huống relay bị kill sau khi gửi nhưng trước khi đánh dấu SENT:
     * cùng một eventId bay lên Kafka HAI lần. Log consumer sẽ cho thấy lần hai bị bỏ qua.
     */
    @PostMapping("/demo/duplicate")
    public Map<String, String> sendDuplicate() {
        String eventId = UUID.randomUUID().toString();
        String payload = "{\"orderId\":-1,\"product\":\"duplicate-demo\",\"quantity\":1}";
        kafkaTemplate.executeInTransaction(template -> {
            for (int i = 0; i < 2; i++) {
                template.send(MessageBuilder.withPayload(payload)
                        .setHeader(KafkaHeaders.TOPIC, topic)
                        .setHeader(KafkaHeaders.KEY, "duplicate-demo")
                        .setHeader("eventId", eventId)
                        .setHeader("eventType", "OrderPlaced")
                        .build());
            }
            return null;
        });
        return Map.of("eventId", eventId,
                "message", "đã gửi 2 bản cùng eventId — xem log consumer: 1 lần xử lý, 1 lần bỏ qua");
    }
}
