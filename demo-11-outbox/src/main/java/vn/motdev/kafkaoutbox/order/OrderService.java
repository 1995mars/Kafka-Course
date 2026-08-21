package vn.motdev.kafkaoutbox.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import vn.motdev.kafkaoutbox.outbox.OutboxEvent;
import vn.motdev.kafkaoutbox.outbox.OutboxEventRepository;

/**
 * Trái tim của Transaction Outbox: {@code @Transactional} ôm CẢ HAI lệnh insert.
 *
 * <p>Hoặc đơn hàng VÀ event cùng được commit, hoặc cùng rollback — không tồn tại trạng thái
 * "Kafka nhận event của một đơn hàng chưa từng tồn tại". Đây là thứ Publisher Service
 * (ghi RocksDB riêng) không làm được, vì RocksDB không tham gia transaction của DB nghiệp vụ.
 *
 * <p>Chú ý điều KHÔNG có ở đây: không một lệnh gửi Kafka nào. Service chỉ ghi DB;
 * việc publish là của {@code OutboxRelay}, chạy sau, ngoài transaction này.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final PurchaseOrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(PurchaseOrderRepository orderRepository,
                        OutboxEventRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    // Phải gọi đích danh TM của JPA: từ lúc bật transaction-id-prefix, Boot tạo thêm
    // kafkaTransactionManager và @Transactional "trần" không biết chọn ai trong hai.
    @Transactional("transactionManager")
    public PurchaseOrder placeOrder(String product, int quantity, boolean failAfterWrite) {
        PurchaseOrder order = orderRepository.save(new PurchaseOrder(product, quantity));

        OutboxEvent event = new OutboxEvent(
                String.valueOf(order.getId()), "OrderPlaced", buildPayload(order));
        outboxRepository.save(event);
        log.info("[ORDER  ] ghi đơn #{} + outbox event {} trong CÙNG transaction",
                order.getId(), event.getEventId());

        // Giả lập bug/exception xảy ra SAU khi cả hai bảng đã được ghi nhưng TRƯỚC commit:
        // transaction rollback → cả đơn hàng lẫn event đều biến mất, Kafka không nhận gì.
        if (failAfterWrite) {
            throw new IllegalStateException("giả lập lỗi nghiệp vụ trước commit — rollback cả hai bảng");
        }
        return order;
    }

    private String buildPayload(PurchaseOrder order) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "orderId", order.getId(),
                    "product", order.getProduct(),
                    "quantity", order.getQuantity()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("không serialize được payload", e);
        }
    }
}
