# Buổi 10 — Testing Kafka với Spring Boot

Cùng một ứng dụng (producer + consumer + `CountDownLatch`), test bằng **hai** cách dựng broker.

| | Embedded Kafka | Testcontainers |
|---|---|---|
| Broker | in-memory, trong JVM test | Kafka thật trong Docker |
| Tốc độ | nhanh | chậm hơn (kéo image, khởi động container) |
| Cần Docker | không | có |
| Độ sát production | gần đúng | đúng |
| Dùng khi | vòng lặp dev, CI không có Docker | pipeline chính trước khi release |

Code ứng dụng **y hệt nhau** ở cả hai — chỉ khác cách dựng broker và cách trỏ
`spring.kafka.bootstrap-servers` vào đó.

## Chạy test

```bash
mvn test                                  # chạy cả hai
mvn test -Dtest=EmbeddedKafkaTest         # chỉ embedded, không cần Docker
mvn test -Dtest=TestcontainersKafkaTest   # cần Docker đang chạy
```

Không có Docker thì `TestcontainersKafkaTest` **tự bỏ qua** (nhờ `disabledWithoutDocker = true`)
chứ không làm đỏ cả build.

## Mấu chốt: chờ thế nào cho đúng

Message đi qua broker rồi mới tới listener, trên một thread khác — assert ngay sau khi gửi thì
luôn trượt. Cách chuẩn là `CountDownLatch(1)`: listener nhận được thì `countDown()`, test gọi
`latch.await(10, SECONDS)`.

```java
producer.send(topic, "xin chào Kafka");
assertThat(consumer.await(10)).isTrue();     // false = hết giờ = test fail rõ ràng
assertThat(consumer.getPayload()).isEqualTo("xin chào Kafka");
```

Đừng thay bằng `Thread.sleep(2000)`: chậm thì phí thời gian, nhanh thì test chập chờn.

## Hai chi tiết dễ làm test treo

- **`auto-offset-reset: earliest`** — listener thường sẵn sàng *sau* khi test đã gửi message.
  Để `latest` là message đó bị bỏ qua và test đứng chờ tới hết timeout.
- **Đừng ghim cổng cho Embedded Kafka.** Tài liệu gốc đặt `brokerProperties` listener ở
  `localhost:9092`; cổng đó đụng ngay nếu máy đang chạy Kafka bằng Docker cho các buổi khác.
  Ở đây dùng cổng tự chọn rồi bơm vào context:

  ```java
  @SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
  ```

  Testcontainers cũng dùng cổng động, và `@DynamicPropertySource` là cách gọn nhất để đưa
  `kafka.getBootstrapServers()` vào Environment — không cần class `@Configuration` riêng như
  bản `@ClassRule` cũ.

## Chiến lược thực dụng

Embedded cho phần lớn test chạy hằng ngày; Testcontainers cho pipeline chính trước khi release.
Nguyên tắc chung: test phải **self-contained** — tự dựng, tự dọn. Trỏ test vào broker dev dùng
chung là công thức cho test chập chờn.
