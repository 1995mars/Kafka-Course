# Buổi 9 — Error Handling, Retry & Recovery

Hai trường phái xử lý lỗi chạy song song trong cùng một ứng dụng, để so sánh trực tiếp:

```
payments ──► DefaultErrorHandler (BLOCKING)          orders ──► @RetryableTopic (NON-BLOCKING)
             backoff 1s → 2s, tối đa 2 retry                    attempts=4, backoff 1s → 2s → 4s
                    │                                                  │
        ┌───────────┴────────────┐                          orders-retry-0/1/2 (Spring tự tạo)
        ▼                        ▼                                     │
  payments-retry            payments-dlt                          orders-dlt  ──► @DltHandler
  (lỗi phục hồi được)       (lỗi dữ liệu)
```

Cộng thêm phần thường bị bỏ quên: **xử lý lỗi phía producer** (`ResilientEventPublisher`).

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker

## Chạy demo

```bash
docker compose up -d
mvn spring-boot:run
```

## Bốn kịch bản lỗi — bấm và đọc log

| Lệnh | Chuyện gì xảy ra |
|---|---|
| `curl -X POST "localhost:8080/api/payments?type=ok"` | xử lý thành công ngay |
| `curl -X POST "localhost:8080/api/payments?type=flaky"` | hỏng 2 lần, **retry cứu được** ở lần 3 — không rời topic gốc |
| `curl -X POST "localhost:8080/api/payments?type=recoverable"` | hỏng cả 3 lượt → sang **payments-retry** → xử lý lại và thành công |
| `curl -X POST "localhost:8080/api/payments?type=invalid"` | `IllegalArgumentException` → **không retry lần nào** → thẳng **payments-dlt** |

Kịch bản `flaky` in ra rõ nhịp backoff — để ý dấu thời gian giữa các lần thử:

```
[PAYMENT ] nhận flaky-0002 (partition=1 offset=0)
[RETRY   ] lần thử 1 thất bại cho flaky-0002 | kết nối chập chờn, lần thử 1
   ...1 giây sau...
[RETRY   ] lần thử 2 thất bại cho flaky-0002 | kết nối chập chờn, lần thử 2
   ...2 giây sau...
[PAYMENT ] ✅ xử lý thành công flaky-0002 (lần thử 3)
```

Còn `invalid` đi thẳng, không chờ giây nào — đó là tác dụng của `addNotRetryableExceptions`:
lỗi dữ liệu thì retry một triệu lần vẫn lỗi, chỉ tốn tài nguyên.

## @RetryableTopic — retry mà không chặn luồng chính

```bash
curl -X POST "localhost:8080/api/orders?type=flaky"   # thành công ở retry topic thứ nhất
curl -X POST "localhost:8080/api/orders?type=fail"    # cạn 4 lượt → orders-dlt → @DltHandler
curl -X POST "localhost:8080/api/orders?type=invalid" # không nằm trong include → thẳng orders-dlt
```

Cột `topic` trong log cho thấy message dịch chuyển: `orders` → `orders-retry-0` → `orders-retry-1`…
Đó chính là điểm ăn tiền: trong lúc message hỏng nằm chờ ở topic retry, partition của topic
`orders` vẫn chạy tiếp — không ai bị kẹt sau lưng nó.

Xem các topic Spring tự sinh (Windows + Git Bash: chạy trong PowerShell hoặc thêm
`MSYS_NO_PATHCONV=1`):

```bash
docker exec kafka-buoi09 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
```

## Lỗi phía producer

```bash
curl -X POST "localhost:8080/api/producer-failure"
```

Gửi vào topic không tồn tại → producer kiên trì đến hết `delivery.timeout.ms` (8 giây) rồi ném
`TimeoutException`. Event được cất vào hàng chờ và scheduler gửi lại mỗi 10 giây, tối đa 3 lần
rồi bỏ cuộc kèm log cảnh báo.

Đừng đẩy event gửi hỏng vào một *retry topic*: nguyên nhân hỏng thường là cả cluster có vấn đề,
mà retry topic cũng nằm trên cluster đó. Lưu vào **database** rồi để scheduler gửi lại mới đáng
tin — và đó chính là phôi thai của Transaction Outbox ở Buổi 11.

## Đổi kiểu backoff

Thêm `demo.backoff: fixed` vào `application.yml` để dùng `FixedBackOff(1000, 2)` thay cho
`ExponentialBackOffWithMaxRetries`. Cấu hình thật nên có interval lớn hơn nhiều và **jitter**
— chút ngẫu nhiên để hàng trăm consumer không cùng retry vào một nhịp.

## Ghi nhớ

- Phân loại **retryable** với **non-retryable** trước khi viết dòng code nào.
- Listener chỉ ném exception; hạ tầng (`DefaultErrorHandler`) quyết định retry hay bỏ.
- Retry blocking làm đứng partition — `@RetryableTopic` tránh được điều đó.
- DLT không phải thùng rác: phải có listener log đủ ngữ cảnh và cảnh báo khi tỷ lệ lỗi tăng.
- **Tên header khác nhau giữa hai đường:** `@RetryableTopic` ghi nguyên nhân vào
  `kafka_exception-message`, còn `DeadLetterPublishingRecoverer` tự cấu hình ghi vào
  `kafka_dlt-exception-message`. Tra nhầm hằng số thì `@DltHandler` in ra rỗng mà không báo lỗi.
- Giám sát **consumer lag** — chuông báo động sớm nhất khi luồng xử lý có vấn đề.

## Dọn dẹp

```bash
docker compose down -v
```
