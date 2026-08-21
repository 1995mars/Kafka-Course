# Buổi 7 — Quản lý Offset với Spring Boot

Cùng một topic `orders`, nhiều cách trả lời câu hỏi **"offset được commit khi nào?"**.

| Chạy với profile | Bạn thấy gì |
|---|---|
| *(mặc định)* | 3 listener, 3 AckMode: `BATCH` · `RECORD` · `MANUAL_IMMEDIATE` |
| `pitfall` | Cạm bẫy thread pool: bản SAI làm mất message vs bản ĐÚNG |
| `plain-client` | Kafka client thuần: auto · `commitSync` · `commitAsync` · commit offset cụ thể |

Mỗi listener một `groupId` riêng nên cả ba cùng nhận đủ 100% message — so sánh được trực tiếp.
Topic để **1 partition** và `max-poll-records: 5` để batch nhỏ, ranh giới commit dễ nhìn.

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker

## Chạy demo

```bash
docker compose up -d
mvn spring-boot:run

# bơm 10 đơn hàng (mỗi đơn xử lý mất 1.5 giây — cố tình chậm)
curl -X POST "http://localhost:8080/api/orders?count=10"
```

```
[BATCH  ] ▶ bắt đầu xử lý ORD-0001 (offset=0)
[RECORD ] ▶ bắt đầu xử lý ORD-0001 (offset=0)
[MANUAL ] ▶ bắt đầu xử lý ORD-0001 (offset=0)
[BATCH  ] ✔ XỬ LÝ XONG ORD-0001 (offset=0)
...
```

## Thí nghiệm chính: giết tiến trình giữa batch

Đây là phần đáng giá nhất của buổi học — commit offset chỉ lộ mặt khi có sự cố.

```bash
# 1. Gửi 10 đơn rồi ĐỢI vài giây cho consumer xử lý được 2-3 đơn
curl -X POST "http://localhost:8080/api/orders?count=10"

# 2. Giết KHÔNG kịp dọn dẹp (Windows: taskkill /F /PID <pid>)
kill -9 $(jps -l | grep kafka-offset-demo | cut -d' ' -f1)

# 3. Xem offset đã commit của từng group
docker exec kafka-buoi07 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --all-groups

# 4. Bật lại và xem đơn nào chạy lại
mvn spring-boot:run
```

> Windows + Git Bash: chạy lệnh `docker exec … /opt/kafka/bin/…` trong PowerShell, hoặc thêm
> `MSYS_NO_PATHCONV=1` phía trước — Git Bash đổi `/opt/...` thành đường dẫn Windows và báo
> `no such file or directory`.

Kết quả điển hình: `order-record` có `CURRENT-OFFSET` nhỉnh hơn `order-batch`, vì RECORD
commit sau từng bản ghi còn BATCH phải xong cả batch 5 bản ghi mới commit một lần. Sau khi
restart, group `order-batch` xử lý lại nhiều đơn hơn — **duplicate**. Không có cấu hình nào
xoá được duplicate hoàn toàn; cách chống thật sự là làm business logic idempotent.

## Thí nghiệm: cạm bẫy thread pool

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local,pitfall
curl -X POST "http://localhost:8080/api/orders?count=10"
```

`[UNSAFE ]` return ngay sau khi giao việc cho thread pool → Spring commit offset trong khi
worker còn đang chạy. Giết tiến trình lúc này: các đơn đó **mất vĩnh viễn**, restart không
đọc lại vì offset đã commit. `[SAFE   ]` dùng `MANUAL_IMMEDIATE` và chỉ `acknowledge()` ở
dòng cuối của worker → restart sẽ xử lý lại, không mất.

## Thí nghiệm: Kafka client thuần

```bash
# đổi demo.commit-strategy trong application.yml: auto | sync | async | specific
mvn spring-boot:run -Dspring-boot.run.profiles=local,plain-client
```

Đọc code `PlainClientCommitDemo` song song với log — đặc biệt là `offset + 1` ở chiến lược
`specific`: offset ta commit là vị trí sẽ **đọc tiếp**, không phải offset vừa xử lý.

## Ghi nhớ

- Spring Kafka mặc định `enable.auto.commit = false` và tự commit theo **AckMode** (mặc định `BATCH`).
- `RECORD` an toàn hơn, `BATCH` nhanh hơn; `MANUAL_IMMEDIATE` khi bạn cần tự quyết định điểm commit.
- Xử lý bất đồng bộ mà vẫn để framework commit = kịch bản mất dữ liệu.
- `commitAsync` không tự retry — kết hợp `commitAsync` khi chạy + `commitSync` trước khi đóng.
- `spring.kafka.listener.immediate-stop: true` khi mỗi message tốn nhiều giây và shutdown bị giới hạn thời gian.

## Dọn dẹp

```bash
docker compose down -v
```
