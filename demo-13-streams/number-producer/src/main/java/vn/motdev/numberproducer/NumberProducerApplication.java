package vn.motdev.numberproducer;

import java.time.Duration;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Flux;

/**
 * Microservice 1/3 — Producer: bean kiểu {@link Supplier} là TẤT CẢ những gì cần viết,
 * Spring Cloud Stream nhìn thấy Supplier là hiểu "đây là producer" và tự nối nó vào
 * binding {@code numberProducer-out-0} → topic {@code numbers} (xem application.yml).
 *
 * <p>Luồng ở đây VÔ HẠN nên dùng kiểu reactive {@link Flux}: range 1→1000, mỗi giây
 * nhả một số. Ngoài đời dòng số này là dòng sự kiện hành vi người dùng.
 * Không một dòng KafkaTemplate hay Kafka client nào — framework lo phần còn lại.
 */
@SpringBootApplication
public class NumberProducerApplication {

    private static final Logger log = LoggerFactory.getLogger(NumberProducerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(NumberProducerApplication.class, args);
    }

    @Bean
    public Supplier<Flux<Long>> numberProducer() {
        return () -> Flux.range(1, 1000)
                .map(Integer::longValue)
                .delayElements(Duration.ofSeconds(1))
                .doOnNext(n -> log.info("[PRODUCER ] → numbers: {}", n));
    }
}
