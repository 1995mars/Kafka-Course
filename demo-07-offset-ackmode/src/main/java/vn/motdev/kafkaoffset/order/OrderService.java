package vn.motdev.kafkaoffset.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * "Business logic" của demo: xử lý một đơn hàng mất {@code app.processing-millis}.
 *
 * <p>Cố tình chậm để bạn kịp Ctrl+C hoặc kill -9 vào GIỮA một batch — đó là lúc
 * ranh giới commit offset lộ ra rõ nhất.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final long processingMillis;

    public OrderService(@Value("${app.processing-millis}") long processingMillis) {
        this.processingMillis = processingMillis;
    }

    public void process(String tag, String order, long offset) {
        log.info("{} ▶ bắt đầu xử lý {} (offset={})", tag, order, offset);
        try {
            Thread.sleep(processingMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("bị ngắt khi đang xử lý " + order, e);
        }
        log.info("{} ✔ XỬ LÝ XONG {} (offset={})", tag, order, offset);
    }
}
