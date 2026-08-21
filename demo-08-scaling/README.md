# Buổi 8 — Scaling Consumers trong Consumer Group

Producer bắn **1 message/giây**. Consumer xử lý mất **2 giây/message**. Một mình nó thua —
và ta chữa bằng cách thêm instance vào cùng consumer group, cho tới khi chạm trần: **số partition**.

```
producer ──► sequential-number (3 partition) ──► group "number-consumer"
   1/giây                                          consumer × N  (2 giây/message)
```

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker

## Build & chạy

```bash
# 1. Build jar cho cả hai ứng dụng (Dockerfile chỉ COPY jar, không build trong image)
mvn -f producer/pom.xml clean package
mvn -f consumer/pom.xml clean package

# 2. Bước một — 1 consumer
docker compose up --build --scale consumer=1
```

Xem log producer đếm 1, 2, 3… . Sau ~1 phút producer


```bash
# 3. Bước hai — 2 consumer (mở terminal khác)
docker compose up -d --scale consumer=2

# 4. Bước ba — 3 consumer: mỗi con đúng 1 partition, message được xử lý gần như tức thì
docker compose up -d --scale consumer=3

# 5. Bước bốn — 4 consumer: con thứ tư KHÔNG được gán partition nào
docker compose up -d --scale consumer=4

docker compose logs -f consumer
```

Mỗi lần scale, log rebalance nói thẳng ai nhận gì:

```
[c1a2b3] ✅ được gán 2 partition: [sequential-number-0, sequential-number-1]
[d4e5f6] ✅ được gán 1 partition: [sequential-number-2]
[a7b8c9] ⚠ KHÔNG được gán partition nào — số consumer đã vượt số partition, instance này sẽ ngồi không
```

## consumer lag

```bash
docker exec kafka-buoi08 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --group number-consumer
```

> Windows + Git Bash: chạy lệnh này trong PowerShell hoặc thêm `MSYS_NO_PATHCONV=1` phía trước.

Cột `LAG` là số message đã vào topic mà group chưa xử lý. Với 1 consumer, LAG tăng đều;
lên 3 consumer, LAG tụt về gần 0. Đây là chỉ số cảnh báo sớm quan trọng nhất khi vận hành.

## Vì sao không "sau này tăng partition cũng được"?

Routing theo key là `hash(key) % số_partition`. Đổi số partition là đổi mẫu số: message cũ
nằm yên chỗ cũ, message mới cùng key lại rơi sang partition khác → **thứ tự theo key vỡ**.
Nếu ordering quan trọng, hãy chốt số partition từ đầu, tính dư cho nhu cầu scale tương lai.

## Một chi tiết nếu không có thì demo này vô nghĩa

Producer bật `partitioner.class = RoundRobinPartitioner`. Partitioner **mặc định** của Kafka là
*sticky*: message không key sẽ dồn vào một partition cho tới khi đủ `batch.size` (16KB) rồi mới
đổi. Với 1 message vài byte mỗi giây, cả buổi demo sẽ nằm gọn trong **một** partition — scale
lên 3 consumer cũng chỉ một con làm việc, hai con còn lại ngồi không mà không hiểu vì sao.

Đây là điều dễ vấp trên production: thêm consumer mà lag không giảm, hoá ra tải không hề trải
đều trên các partition.

## Chỉnh tốc độ xử lý

Sửa `MESSAGE_PROCESSING_TIME` trong `docker-compose.yml` (ms): để `500` thì một consumer
thừa sức theo kịp producer — không cần scale. Đây là cách kiểm chứng ngược lại: scale out chỉ
là câu trả lời khi consumer thật sự chậm hơn producer.

## Dọn dẹp

```bash
docker compose down -v
```
