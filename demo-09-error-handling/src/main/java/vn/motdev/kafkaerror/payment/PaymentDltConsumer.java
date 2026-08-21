package vn.motdev.kafkaerror.payment;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Hướng recovery thứ hai — BUÔNG CÓ KIỂM SOÁT.
 *
 * <p>Message vào DLT là đã hết cửa tự xử lý; việc của listener này là để lại đủ dấu vết cho
 * người điều tra, chứ tuyệt đối không im lặng nuốt lỗi. Ngoài đời chỗ này còn ghi DB và bắn
 * cảnh báo khi tỷ lệ lỗi vượt ngưỡng.
 *
 * <p>{@code DeadLetterPublishingRecoverer} đính kèm nguyên nhân vào header — tất cả đều là
 * mảng byte, nên phải tự decode.
 */
@Component
public class PaymentDltConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentDltConsumer.class);

    @KafkaListener(topics = "${app.topics.payments-dlt}", groupId = "payment-dlt-monitor")
    public void consume(ConsumerRecord<String, String> record) {
        log.error("""
                        [DLT     ] ☠ message chết: {}
                                   topic gốc : {}
                                   nguyên nhân: {} — {}""",
                record.value(),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                header(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "(không có)" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
