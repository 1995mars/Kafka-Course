package vn.motdev.kafkademo.producer;

import java.util.Map;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Gửi message thủ công để quan sát 3 consumer group cùng nhận:
 *
 *   curl -X POST "http://localhost:8080/api/drivers/007/location?lat=21.028511&lng=105.804817"
 */
@RestController
public class LocationController {

    private final DriverLocationProducer producer;

    public LocationController(DriverLocationProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/api/drivers/{driverId}/location")
    public Map<String, String> publish(@PathVariable String driverId,
                                       @RequestParam double lat,
                                       @RequestParam double lng) {
        producer.sendLocation(driverId, lat, lng);
        return Map.of(
                "status", "published",
                "driverId", driverId,
                "hint", "xem log: 1 message sẽ được cả 3 consumer group xử lý");
    }
}
