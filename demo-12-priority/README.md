# Buổi 12 — Message Prioritization: 3 pattern trong 1 ứng dụng

Kafka không có priority queue (partition là log append-only bất biến — không chen hàng được),
nên ưu tiên phải thiết kế ở tầng ứng dụng. Ba pattern của buổi học, mỗi pattern một package:

```
1. BRUTE-FORCE            notify-high ─┐
   (bruteforce/)                       ├─► vòng lặp poll high trước, sạch mới xuống low
                          notify-low ──┘

2. RESEQUENCER            reseq-in ─► buffer (PriorityQueue, xả khi đầy 10 / hết 5s) ─► reseq-out
   (resequencer/)

3. BUCKET PRIORITY        bucket-orders (6 partitions)
   (bucket/)              ├─ partition 0–3 = bucket HIGH ─► 4 consumer thread
                          └─ partition 4–5 = bucket LOW  ─► 2 consumer thread
```

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker

## Chạy demo

```bash
docker compose up -d
mvn spring-boot:run
```

## Pattern 1 — Brute-force: topic riêng cho từng mức

```bash
curl -X POST "localhost:8080/api/brute/burst?high=10&low=3"
```

Low được gửi **trước**, high gửi sau — nhưng log `[BRUTE]` xử lý hết high rồi mới đến low,
vì vòng lặp (`BruteForceConsumerLoop`) poll topic cao trước và chỉ ngó xuống khi high sạch.

Và đây là lỗ hổng lộ liễu của pattern:

```bash
curl -X POST "localhost:8080/api/brute/starve"
```

High đến đều đặn (250ms/cái) nhanh hơn tốc độ xử lý (300ms/cái) → 5 message low **đói suốt
15 giây**, chỉ được xử lý khi flood ngừng. Topic cao mà có message đều đều thì topic thấp
không bao giờ đến lượt — starvation.

## Pattern 2 — Resequencer: buffer + sắp xếp lại

```bash
curl -X POST "localhost:8080/api/resequencer/burst"
```

12 message priority ngẫu nhiên (key = 1/2/3, 1 cao nhất). Đối chiếu hai nhóm log:
`[RESEQ]` vào buffer theo thứ tự gửi lộn xộn → `[RESEQ-OUT]` ra theo priority tăng dần.
10 cái đầu xả ngay vì **buffer đầy**, 2 cái cuối phải chờ **timeout 5s** — nhìn thấy luôn
điểm trừ của pattern: message VIP cũng phải nằm chờ trong buffer.

(Bản trong tài liệu gốc dùng Apache Camel + `ExpressionResultComparator`; demo tự viết bằng
`PriorityQueue` + `@Scheduled` để lộ rõ cơ chế — logic giữ nguyên.)

## Pattern 3 — Bucket Priority (khuyên dùng)

```bash
curl -X POST "localhost:8080/api/bucket/burst?high=12&low=12"
```

MỘT topic duy nhất, `BucketPriorityPartitioner` đọc prefix của key: `high-*` vào partition
0–3, còn lại vào 4–5. Log `[BUCKET]` cho thấy:

- **Cả hai mức chạy song song ngay từ giây đầu** — không ai bị bỏ đói như brute-force,
  không ai nằm chờ buffer như resequencer.
- High có 4 thread, low có 2 → cùng 12 message, high drain xong sau ~1.5s, low sau ~3s.
  Ưu tiên bằng **phân bổ tài nguyên**, không phải chen hàng.

Cột `thread=` trong log chính là bằng chứng: quy tắc vàng 1 partition = 1 consumer trong
group biến "nhiều partition hơn" thành "nhiều nhân công hơn".

Bản production-grade (bucket theo %, tự co giãn, phía consumer cũng chia bucket):
[bucket-priority-pattern](https://github.com/riferrei/bucket-priority-pattern) của Ricardo Ferreira.

## Ghi nhớ

- Kafka không có priority vì partition là file append-only — đã ghi là bất biến.
- Brute-force: dễ làm, dễ starvation. Resequencer: đổi độ trễ lấy thứ tự. Bucket Priority:
  cân bằng nhất — mọi mức đều tiến, chỉ khác tốc độ.
- Bucket dùng **gán partition tĩnh** (`@TopicPartition`), không dùng group rebalancing —
  bucket nào ôm partition nào là quyết định thiết kế.

## Dọn dẹp

```bash
docker compose down -v
```
