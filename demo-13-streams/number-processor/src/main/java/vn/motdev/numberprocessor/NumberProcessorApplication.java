package vn.motdev.numberprocessor;

import java.util.function.Function;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Microservice 2/3 — Processor: bean kiểu {@link Function} = vừa nhận vừa trả,
 * Spring Cloud Stream hiểu là "processor" và nối in-0 → topic numbers,
 * out-0 → topic squaredNumbers.
 *
 * <p>{@link KStream} là trừu tượng key-value stream của Kafka Streams API — thân hàm
 * đúng hai toán tử như slide: {@code filter} giữ số chẵn, {@code mapValues} bình phương.
 * Ngoài hai toán tử này còn cả thế giới: map, join, groupBy, windowing, aggregate...
 *
 * <p>Ngoài đời, chỗ này là recommendation engine hay rule phát hiện gian lận thẻ —
 * dữ liệu được biến đổi NGAY KHI chảy qua, không chờ batch chạy đêm.
 */
@SpringBootApplication
public class NumberProcessorApplication {

    private static final Logger log = LoggerFactory.getLogger(NumberProcessorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(NumberProcessorApplication.class, args);
    }

    @Bean
    public Function<KStream<String, Long>, KStream<String, Long>> squareEvenNumbers() {
        return input -> input
                .filter((k, v) -> v % 2 == 0)
                .mapValues(v -> v * v)
                .peek((k, v) -> log.info("[PROCESSOR] → squaredNumbers: {}", v));
    }
}
