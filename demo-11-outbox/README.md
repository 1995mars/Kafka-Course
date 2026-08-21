# Buổi 11 — Delivery Guarantees: Transaction Outbox + Idempotent Producer/Consumer

Pipeline "không mất, không trùng" cho nguồn request-driven — ghép cả ba kỹ thuật của buổi học:

```
POST /api/orders
      │  MỘT database transaction
      ▼
┌─────────────────────────────┐      OutboxRelay (2s/lần)        ┌──────────────────────┐
│ purchase_orders  │ outbox   │ ──► idempotent + transactional ──►│ topic order-events   │
│ (nghiệp vụ)      │ (event)  │      producer (acks=all,          │ (3 partitions,       │
└─────────────────────────────┘       enable.idempotence)         │  key = orderId)      │
   cùng commit / cùng rollback                                    └──────────┬───────────┘
                                                                             │ read_committed
                                                                             ▼
                                                              idempotent consumer
                                                              (bảng processed_events khử trùng)
```

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker

## Chạy demo

```bash
docker compose up -d
mvn spring-boot:run
```

## Kịch bản 1 — luồng chuẩn (at-least once đầu-cuối)

```bash
curl -X POST "localhost:8080/api/orders?product=laptop&quantity=2"
```

Đọc log theo thứ tự: `[ORDER  ]` ghi 2 bảng một transaction → tối đa 2 giây sau `[RELAY  ]`
publish → `[CONSUME] ✅`. Soi bảng outbox bằng `curl localhost:8080/api/outbox` (hoặc H2 console
`localhost:8080/h2-console`, JDBC URL `jdbc:h2:mem:outboxdemo`) — event chuyển `PENDING → SENT`.

## Kịch bản 2 — atomicity: rollback là rollback cả hai

```bash
curl -X POST "localhost:8080/api/orders?fail=true"
```

Exception ném ra SAU khi cả hai bảng đã ghi nhưng trước commit. Response trả về count hai bảng
đều không tăng, `[RELAY]` không có gì để gửi — không tồn tại "Kafka nhận event của đơn hàng ma".
Đây chính là điều Publisher Service (lưu RocksDB riêng) không đảm bảo được.

## Kịch bản 3 — Kafka sập cũng không mất message

```bash
docker stop kafka-buoi11
curl -X POST "localhost:8080/api/orders?product=ssd"
curl localhost:8080/api/outbox          # event nằm PENDING, attempts tăng dần mỗi 2s
docker start kafka-buoi11               # chờ ~20s cho broker dậy
curl localhost:8080/api/outbox          # → SENT, và [CONSUME] ✅ xuất hiện trong log
```

Message sống sót vì nó nằm trong DB **trước** khi lên Kafka — điểm khác biệt cốt lõi so với
gửi thẳng từ memory. Chú ý: quá 5 lần thất bại event chuyển `DEAD` (dead letter ngay trong DB)
— demo để ngưỡng thấp cho dễ xem; muốn thấy `DEAD`, cứ để broker tắt ~10 giây có gửi đơn.

## Kịch bản 4 — duplicate bị consumer khử

```bash
curl -X POST "localhost:8080/api/demo/duplicate"
```

Cùng một `eventId` được gửi 2 lần (giả lập relay bị kill giữa "gửi xong" và "đánh dấu SENT").
Log consumer: một dòng `✅ xử lý`, một dòng `⏭️ BỎ QUA duplicate` — exactly-once **thực dụng**:
at-least once + idempotent consumer.

## Ghi nhớ

- `acks=all` + `enable.idempotence=true` + `retries=MAX_INT` + `max.in.flight ≤ 5` là bộ config
  chuẩn cho producer nghiêm túc về độ bền (xem comment trong `application.yml`).
- Idempotent producer chỉ diệt duplicate **do retry** — nó không cứu được message chưa từng
  được ghi xuống đâu bền vững. Nguồn request-driven cần Outbox (hoặc Publisher Service).
- Outbox = atomic với DB nghiệp vụ; đổi lại DB gánh thêm tải và event đến trễ một nhịp relay.
- Relay là at-least once → payload phải mang `eventId` → consumer khử trùng. Ba mảnh này
  đi cùng nhau, thiếu một là hỏng guarantee.
- `isolation.level=read_committed` để consumer không thấy message của transaction bị abort.

## Dọn dẹp

```bash
docker compose down -v
```
