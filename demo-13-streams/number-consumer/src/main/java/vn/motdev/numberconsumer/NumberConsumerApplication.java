package vn.motdev.numberconsumer;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Microservice 3/3 — Consumer: bean kiểu {@link Consumer} = chỉ nhận, không trả —
 * Spring Cloud Stream nối nó vào binding {@code numberConsumer-in-0} → topic
 * {@code squaredNumbers}.
 *
 * <p>Ngoài đời đây là app hiển thị đề xuất cho người dùng. Bộ ba Supplier — Function —
 * Consumer ánh xạ trọn vẹn Producer — Processor — Consumer của Kafka, mà không có
 * một dòng Kafka client nào trong cả ba module.
 */
@SpringBootApplication
public class NumberConsumerApplication {

    private static final Logger log = LoggerFactory.getLogger(NumberConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(NumberConsumerApplication.class, args);
    }

    @Bean
    public Consumer<Long> numberConsumer() {
        return n -> log.info("[CONSUMER ] nhận số bình phương: {}", n);
    }
}
