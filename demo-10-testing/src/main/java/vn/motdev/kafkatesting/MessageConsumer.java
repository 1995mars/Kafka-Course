package vn.motdev.kafkatesting;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer kèm {@link CountDownLatch} — kỹ thuật chuẩn để "chờ" trong thế giới bất đồng bộ.
 *
 * <p>Test không thể assert ngay sau khi gửi: message còn phải đi qua broker rồi mới tới
 * listener, trên một thread khác. Latch cho test một điểm hẹn: {@link #await(long)} trả về
 * true nghĩa là message đã đến, false nghĩa là hết giờ — và đó là một test FAIL rõ ràng,
 * thay vì một {@code Thread.sleep} đoán mò lúc chạy được lúc không.
 *
 * <p>{@code volatile} vì listener ghi trên thread của container còn test đọc trên thread khác.
 */
@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private volatile CountDownLatch latch = new CountDownLatch(1);
    private volatile String payload;

    @KafkaListener(topics = "${app.topic}")
    public void receive(String message) {
        log.info("[CONSUMER] nhận '{}'", message);
        this.payload = message;
        this.latch.countDown();
    }

    public boolean await(long timeoutSeconds) throws InterruptedException {
        return latch.await(timeoutSeconds, TimeUnit.SECONDS);
    }

    public String getPayload() {
        return payload;
    }

    /** Gọi ở đầu mỗi test nếu context được dùng lại cho nhiều test. */
    public void reset() {
        this.latch = new CountDownLatch(1);
        this.payload = null;
    }
}
