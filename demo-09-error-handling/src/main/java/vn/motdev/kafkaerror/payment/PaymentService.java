package vn.motdev.kafkaerror.payment;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

/**
 * "Business logic" của demo, cố tình hỏng theo tiền tố của payload để bạn tự bấm nút gây lỗi:
 *
 * <table border="1">
 *   <caption>Ma trận kịch bản</caption>
 *   <tr><th>Payload</th><th>Hành vi</th><th>Kết cục</th></tr>
 *   <tr><td>ok-*</td><td>thành công ngay</td><td>xử lý xong</td></tr>
 *   <tr><td>flaky-*</td><td>hỏng 2 lần đầu rồi thành công</td><td>retry cứu được, không rời topic gốc</td></tr>
 *   <tr><td>recoverable-*</td><td>hỏng suốt 3 lượt trên topic gốc</td><td>sang retry topic rồi thành công ở đó</td></tr>
 *   <tr><td>invalid-*</td><td>IllegalArgumentException</td><td>không retry, đi thẳng DLT</td></tr>
 * </table>
 *
 * <p>Bộ đếm lần thử để trong bộ nhớ cho gọn — ứng dụng thật sẽ dựa vào dữ liệu nghiệp vụ
 * (trạng thái đơn hàng trong DB) chứ không đếm kiểu này.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    public void process(String payment) {
        int attempt = attempts.computeIfAbsent(payment, key -> new AtomicInteger()).incrementAndGet();

        if (payment.startsWith("invalid")) {
            // Lỗi dữ liệu: KHÔNG bao giờ tự khỏi. Đã khai trong addNotRetryableExceptions
            // nên DefaultErrorHandler bỏ qua backoff và gọi recoverer ngay.
            throw new IllegalArgumentException("payload sai định dạng: " + payment);
        }
        if (payment.startsWith("flaky") && attempt <= 2) {
            throw new RecoverableDataAccessException("kết nối chập chờn, lần thử " + attempt);
        }
        if (payment.startsWith("recoverable") && attempt <= 3) {
            throw new RecoverableDataAccessException("downstream đang sập, lần thử " + attempt);
        }

        log.info("[PAYMENT ] ✅ xử lý thành công {} (lần thử {})", payment, attempt);
    }
}
