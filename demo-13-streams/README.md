# Buổi 13 — Kafka Streams & Spring Cloud Stream: pipeline 3 microservice

Maven multi-module, ba ứng dụng Spring Boot nối nhau qua hai topic — không module nào
viết một dòng Kafka client:

```
number-producer            number-processor                  number-consumer
Supplier<Flux<Long>>       Function<KStream, KStream>        Consumer<Long>
mỗi giây nhả 1 số   ──►  numbers  ──►  bỏ lẻ, bình phương  ──►  squaredNumbers  ──►  in ra
                                       (Kafka Streams)
```

Ngoài đời: producer = hành vi người dùng, processor = recommendation engine / rule chống
gian lận, consumer = app hiển thị. Ba interface hàm Java ánh xạ thẳng ba vai trò Kafka —
`Supplier` → Producer, `Function` → Processor, `Consumer` → Consumer.

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker

## Chạy demo — 3 terminal, theo thứ tự

```bash
docker compose up -d
mvn install -N          # cài parent pom vào local repo (chạy 1 lần)

# Terminal 1
mvn -pl number-consumer  spring-boot:run
# Terminal 2
mvn -pl number-processor spring-boot:run
# Terminal 3
mvn -pl number-producer  spring-boot:run
```

Sau vài giây, mỗi terminal một nhịp:

```
[PRODUCER ] → numbers: 7          (bị processor bỏ — số lẻ)
[PRODUCER ] → numbers: 8
[PROCESSOR] → squaredNumbers: 64
[CONSUMER ] nhận số bình phương: 64
```

Consumer chỉ thấy 4, 16, 36, 64... — số lẻ biến mất ở processor, số chẵn ra đi với
bình phương của nó. Đó là stream processing: biến đổi NGAY KHI dữ liệu chảy qua.

## Đọc code thế nào

Toàn bộ business logic của cả pipeline nằm trong **3 bean**, mỗi module một bean
(xem `*Application.java`). Đáng nhìn nhất là processor:

```java
@Bean
public Function<KStream<String, Long>, KStream<String, Long>> squareEvenNumbers() {
    return input -> input
            .filter((k, v) -> v % 2 == 0)
            .mapValues(v -> v * v);
}
```

Phép màu còn lại nằm hết trong `application.yml` của từng module:

- `spring.cloud.function.definition` — khai tên bean (nhiều bean cách nhau `;`).
- Quy ước binding: `<bean>-out-0` = chiều **ghi**, `<bean>-in-0` = chiều **đọc**;
  mỗi binding trỏ `destination` = tên topic.
- Pipeline được "nối" bằng cách cho out của processor và in của consumer **cùng
  destination** `squaredNumbers` — đổi tên một chỗ là đứt dây chuyền.
- Producer bật `useNativeEncoding` + `LongSerializer`; processor khai `LongSerde`;
  consumer `use-native-decoding` + `LongDeserializer` — ba module thống nhất một
  cách serialize kiểu Kafka gốc, không qua message converter của Spring.

## Thử nghịch

- Tắt consumer 20 giây rồi bật lại — nó đọc bù toàn bộ số đã lỡ (binding có `group`,
  offset được nhớ; xóa `group` đi thì thành anonymous group, tắt là mất).
- Sửa `filter` thành `v % 2 != 0` — pipeline đổi hành vi mà producer/consumer không đổi chữ nào.
- Đổi binder: cùng code Supplier/Function/Consumer này chạy được với RabbitMQ chỉ bằng
  cách thay dependency binder — đó là giá trị cốt lõi của Spring Cloud Stream:
  business code không biết broker là gì.

## Ghi nhớ

- Binder **tự tạo topic** theo destination nếu chưa có (compose bật auto-create) —
  "framework lo hết", nhưng production nên tạo topic chủ động để kiểm soát partition count.
- Kafka Streams bắt buộc `applicationId` — vừa là group.id, vừa là prefix topic nội bộ.
- Thứ tự bật: consumer trước producer để không phải chờ đọc bù; processor đứng giữa
  bật lúc nào cũng được.

## Dọn dẹp

```bash
docker compose down -v
```
