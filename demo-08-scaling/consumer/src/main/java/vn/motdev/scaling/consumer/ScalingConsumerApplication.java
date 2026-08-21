package vn.motdev.scaling.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Consumer chậm của demo scaling — chạy nhiều instance bằng `docker compose --scale consumer=N`. */
@SpringBootApplication
public class ScalingConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ScalingConsumerApplication.class, args);
    }
}
