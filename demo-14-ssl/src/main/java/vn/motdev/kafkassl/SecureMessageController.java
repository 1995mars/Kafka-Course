package vn.motdev.kafkassl;

import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Producer y hệt Buổi 6 — điểm cần thấy chính là chỗ KHÔNG có gì để thấy:
 * bật SSL không đổi một dòng code gửi message nào.
 */
@RestController
public class SecureMessageController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public SecureMessageController(KafkaTemplate<String, String> kafkaTemplate,
                                   @Value("${app.topics.secure-messages}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @PostMapping("/api/secure-send")
    public Map<String, String> send(@RequestParam(defaultValue = "hello qua SSL") String message) {
        kafkaTemplate.send(topic, message);
        return Map.of("message", "đã gửi qua kênh SSL: " + message);
    }
}
