# Buổi 14 — Kafka Security với SSL

Broker chỉ mở cổng SSL ra ngoài; client (Spring Boot lẫn CLI) phải mang truststore mới nói
chuyện được. Code producer/consumer không đổi một dòng so với demo thường — toàn bộ nằm ở config.

```
ssl/create-certs.sh          docker compose            Spring Boot app
┌──────────────────┐   ┌──────────────────────┐   ┌─────────────────────────┐
│ 1. CA (ca.crt)   │   │ broker keystore:     │   │ security.protocol: SSL  │
│ 2. broker CSR    │──►│  private key + cert  │◄──│ truststore chứa CA      │
│ 3. CA ký cert    │   │  đã được CA ký       │   │ (application.yml)       │
│ 4. truststore    │   │ listener SSL :9092   │   └─────────────────────────┘
└──────────────────┘   └──────────────────────┘
```

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker · `openssl` + `keytool` (Git Bash + JDK là đủ)

## Chạy demo

```bash
# 1. Sinh chứng chỉ (một lần) — 5 bước trong script khớp 1:1 với slide
bash ssl/create-certs.sh

# 2. Bật broker (mount ssl/secrets vào /etc/kafka/secrets)
docker compose up -d

# 3. Chạy app rồi gửi thử
mvn spring-boot:run
curl -X POST "localhost:8080/api/secure-send?message=du-lieu-mat"
```

Log hiện `[SSL] 🔒 nhận ...` — message đã đi một vòng producer → broker → consumer,
toàn bộ trên kênh mã hóa. Điều đáng chú ý là mọi thứ *trông y như demo thường*: SSL
handshake (bất đối xứng trao Session Key) và mã hóa dữ liệu (đối xứng) chạy ngầm dưới
Kafka client.

## Nhìn thấy sự khác biệt: client không có SSL bị từ chối

CLI trong container, **không** đưa config SSL — nói plaintext vào listener SSL:

```bash
docker exec kafka-buoi14 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
# → treo rồi timeout, broker log cảnh báo "plaintext connection to SSL listener"
```

Cũng lệnh đó, đưa thêm truststore (`client-ssl.properties` do script sinh sẵn):

```bash
docker exec kafka-buoi14 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list \
  --command-config /etc/kafka/secrets/client-ssl.properties
# → liệt kê topic bình thường
```

(Windows + Git Bash: chạy 2 lệnh trên trong PowerShell hoặc thêm `MSYS_NO_PATHCONV=1`.)

## Ba nhân vật — nằm ở đâu trong demo

| Nhân vật | Vai | File |
|---|---|---|
| CA | bên thứ ba đáng tin, KÝ chứng chỉ | `ssl/secrets/ca.crt` + `ca.key` |
| Keystore | két riêng của broker: private key + cert CỦA CHÍNH NÓ (đã ký) | `kafka.broker.keystore.jks` |
| Truststore | danh bạ niềm tin của client: chứa CA | `kafka.client.truststore.jks` |

## Ghi nhớ

- Client chỉ cần **truststore**. Keystore phía client chỉ xuất hiện khi bật
  `ssl.client.auth=required` — mutual TLS, broker xác thực ngược lại client
  (đổi một dòng trong docker-compose + thêm keystore cho app là thành demo mTLS).
- Chứng chỉ broker phải có **SAN** khớp hostname client dùng (`localhost`, `kafka`) —
  script ký bằng `-extfile` vì openssl mặc định vứt extension của CSR; thiếu SAN là client
  báo `SSLHandshakeException: No subject alternative names`.
- SSL tốn CPU cho mã hóa và làm tăng nhẹ latency — bật cho listener ra ngoài, còn
  inter-broker trong mạng tin cậy có thể giữ PLAINTEXT như demo này.
- TLS là tên chuẩn hiện đại; cấu hình Kafka vẫn gọi `ssl.*` theo thói quen cả ngành.
- Demo này mã hóa + xác thực broker. Bước tiếp theo của security là **authorization**
  (ACL) và SASL — nằm ngoài phạm vi buổi học.

## Dọn dẹp

```bash
docker compose down -v
rm -rf ssl/secrets
```
