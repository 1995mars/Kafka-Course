# Khóa học Apache Kafka — tài liệu & code mẫu

Khóa 14 buổi ("buổi") chia làm 3 phần. Tài liệu giảng dạy và code demo nằm cùng thư mục này.

## Tài liệu

| File | Nội dung |
|---|---|
| `Kafka-Course-Script.md` | Đánh giá tài liệu gốc, bảng ánh xạ chương → buổi, script thu âm từng slide |
| `Kafka-Course-Slides.pptx` | 132 slide, 61 diagram nhúng (build lại bằng `diagram-src/build_deck.py`) |
| `diagrams/` | 61 PNG phong cách Excalidraw, tên `s<slide-index>-<topic>.png` |
| `diagram-src/` | Pipeline sinh diagram: `d1/d2/d3.py` → `gen_diagrams.py` → screenshot → `crop_diagrams.py` |

## Code demo

Mỗi thư mục là một project Maven độc lập (Java 21 · Spring Boot 3.3.5 · Kafka KRaft trong Docker),
chạy được ngay và có README riêng mô tả kịch bản demo.

| Demo | Buổi | Minh họa |
|---|---|---|
| `demo-multi-group/` | 2–3 | 1 message → 3 consumer group cùng nhận đủ 100% |
| `demo-06-first-app/` | 6 | Ứng dụng đầu tiên: `KafkaTemplate` + `@KafkaListener` + `NewTopic` |
| `demo-07-offset-ackmode/` | 7 | AckMode BATCH/RECORD/MANUAL, cạm bẫy thread pool, commit bằng client thuần |
| `demo-08-scaling/` | 8 | Consumer group chia tải, `docker compose --scale`, trần = số partition |
| `demo-09-error-handling/` | 9 | Backoff, retry topic, DLT, `@RetryableTopic`, lỗi phía producer |
| `demo-10-testing/` | 10 | Embedded Kafka và Testcontainers |
| `demo-11-outbox/` | 11 | Transaction Outbox (H2 + JPA), idempotent/transactional producer, idempotent consumer |
| `demo-12-priority/` | 12 | 3 pattern ưu tiên: brute-force (+ starvation), resequencer, bucket priority |
| `demo-13-streams/` | 13 | Spring Cloud Stream multi-module: Supplier → Function (KStream) → Consumer |
| `demo-14-ssl/` | 14 | Sinh CA/keystore/truststore bằng keytool, broker SSL, client qua SSL |

**Lưu ý:** các demo đều map cổng `9092` ra host — chạy một demo tại một thời điểm, hoặc
`docker compose down` demo cũ trước khi bật demo mới.

**Windows + Git Bash:** các lệnh `docker exec kafka-... /opt/kafka/bin/*.sh` sẽ lỗi
`no such file or directory` vì Git Bash tự đổi `/opt/...` thành đường dẫn Windows. Chạy chúng
trong PowerShell, hoặc thêm tiền tố `MSYS_NO_PATHCONV=1`.

### Chạy nhanh một demo

```bash
cd demo-07-offset-ackmode
docker compose up -d
mvn spring-boot:run
```

Riêng `demo-06-first-app` có hai project con (`kafka-producer`, `kafka-consumer`),
`demo-08-scaling` phải `mvn package` trước rồi mới `docker compose up --build`,
`demo-13-streams` là Maven multi-module chạy 3 app ở 3 terminal, và `demo-14-ssl`
phải chạy `bash ssl/create-certs.sh` trước khi `docker compose up`.

## Bố cục 14 buổi

- **Phần 1 — Nền tảng (1–5):** message broker, kiến trúc Kafka, producer/consumer, delivery
  semantics & offset, thiết kế partition.
- **Phần 2 — Spring Boot (6–10):** ứng dụng đầu tiên, quản lý offset, scaling, error handling,
  testing. *(Toàn bộ code mẫu Phần 2 nằm trong các thư mục `demo-06` → `demo-10`.)*
- **Phần 3 — Nâng cao (11–14):** delivery guarantees & outbox, prioritization, Kafka Streams &
  Spring Cloud Stream, security SSL + tổng kết. *(Code mẫu trong `demo-11` → `demo-14`.)*
