# Buổi 6 — Ứng dụng Producer–Consumer đầu tiên với Spring Boot

Hai project Spring Boot riêng biệt, đúng như bài giảng: một bên gửi, một bên nhận.

```
kafka-producer                     random-number                    kafka-consumer
  @Scheduled(fixedRate=1000)  ──►   partition 0  ─┐
  kafkaTemplate.sendDefault()       partition 1  ─┼──►  @KafkaListener  ──► console
                                    partition 2  ─┘      group: random-consumer
```

Mảnh ghép chạm tới: `KafkaTemplate` · `@KafkaListener` · cấu hình yml (và bản Java config
tương đương) · `KafkaAdmin` + `NewTopic` tạo topic bằng code · bắt lỗi gửi bằng `whenComplete`.

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker

## Chạy demo

```bash
# 1. Bật Kafka (KRaft, single node, cổng 9092)
docker compose up -d

# 2. Terminal A — producer (tự tạo topic random-number 3 partition rồi bắn mỗi giây 1 số)
cd kafka-producer && mvn spring-boot:run

# 3. Terminal B — consumer
cd kafka-consumer && mvn spring-boot:run
```

## Kết quả mong đợi

Producer:

```
[PRODUCER] gửi 42 → topic=random-number partition=0 offset=0
[PRODUCER] gửi 7  → topic=random-number partition=0 offset=1
[PRODUCER] gửi 88 → topic=random-number partition=0 offset=2
```

Consumer — cùng giá trị, cùng partition/offset, đến gần như tức thì:

```
[CONSUMER] nhận 42 ← partition=0 offset=0
[CONSUMER] nhận 7  ← partition=0 offset=1
[CONSUMER] nhận 88 ← partition=0 offset=2
```

**Vì sao chỉ thấy một partition?** Topic có 3 partition, nhưng message không có key nên Kafka
dùng partitioner *sticky*: nó bám một partition cho tới khi gom đủ `batch.size` (mặc định 16KB)
rồi mới đổi sang partition khác — gộp mạng như vậy hiệu quả hơn nhiều. Demo bắn mỗi giây vài
byte, nên còn lâu mới đầy batch. Đây là hành vi đúng của Kafka, không phải cấu hình sai; xem
phần thí nghiệm bên dưới để rải message ra cả 3 partition.

Thứ tự message chỉ được Kafka đảm bảo **trong một partition**.

## Thí nghiệm thêm

- **`auto-offset-reset` làm gì?** Tắt consumer, để producer chạy tiếp ~10 giây, rồi bật lại
  consumer: nó đọc bù đủ những số đã lỡ, vì offset của group `random-consumer` được lưu trên
  broker. Muốn thấy khác biệt thật sự thì đổi `group-id` sang tên mới rồi so `earliest` với
  `latest` — group mới chưa có offset nào nên `latest` sẽ bỏ qua toàn bộ lịch sử.
- **Gửi kèm key:** đổi `sendDefault(value)` thành
  `kafkaTemplate.send("random-number", String.valueOf(value), value)` → partition được chọn theo
  `hash(key) % 3`, message bắt đầu rải ra cả ba. Dùng key cố định (`"driver-7"`) thì ngược lại:
  mọi message của khóa đó luôn vào **cùng** một partition — đó là cách Kafka giữ thứ tự theo khóa.
- **Ép round-robin:** thêm vào `application.yml` của producer:
  ```yaml
  spring.kafka.producer.properties.partitioner.class: org.apache.kafka.clients.producer.RoundRobinPartitioner
  ```
  Mỗi message sẽ đi một partition khác nhau — demo Buổi 8 bắt buộc phải bật cái này thì việc
  scale consumer mới có tác dụng.
- **Bản Java config:** chạy producer bằng
  `mvn spring-boot:run -Dspring-boot.run.profiles=local,java-config` để dùng
  `ProducerJavaConfig` thay cho phần `spring.kafka.producer` trong yml — kết quả y hệt.
- **Bỏ bean `NewTopic`:** chạy producer với profile khác `local`
  (`-Dspring-boot.run.profiles=default`) khi topic chưa tồn tại → gửi lỗi, vì compose đã tắt
  `auto.create.topics.enable`. Đây chính là lý do production phải tạo topic chủ động.
- **Xem topic vừa tạo:**
  ```bash
  docker exec kafka-buoi06 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:19092 --describe --topic random-number
  ```

## Dọn dẹp

```bash
docker compose down -v
```

> Các demo trong khóa đều dùng cổng 9092 — chạy một demo tại một thời điểm, hoặc
> `docker compose down` demo cũ trước khi bật demo mới.
