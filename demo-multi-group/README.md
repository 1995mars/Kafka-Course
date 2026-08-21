# Demo: 1 Producer + 3 Consumer Group — cùng nhận đủ 100% message

Minh họa cho ví dụ **tracking tài xế Grab** (Buổi 2) và **Queue & Topic bằng consumer group** (Buổi 3):
publish **một** message vào topic `driver_gps` → **cả 3** consumer group cùng xử lý message đó.

```
                                        ┌──────────────────────────────────┐
driver 007 ──► Producer ──► driver_gps ─┼─► [map-service]           đọc 100%
                            (3 partition)├─► [notification-service]  đọc 100%
                                        └─► [working-hours-service] đọc 100%
```

**Vì sao 1 message đến được cả 3 nơi?** Kafka không phải queue "lấy ra là mất" — message nằm
trên log của topic, việc đọc không xóa nó. Mỗi service khai báo một `groupId` **khác nhau**,
mỗi group tự giữ offset riêng, nên group nào cũng đọc trọn vẹn luồng dữ liệu.
(Quy tắc "1 message chỉ đến 1 consumer" chỉ áp dụng **giữa các consumer trong cùng group**.)

## Yêu cầu

- Java 21 · Maven 3.9+ · Docker

## Chạy demo

```bash
# 1. Bật Kafka (KRaft, single node, port 9092)
docker compose up -d

# 2. Chạy ứng dụng (tạo sẵn topic driver_gps 3 partition, bật 1 producer + 3 consumer)
mvn spring-boot:run

# 3. Publish MỘT message
curl -X POST "http://localhost:8080/api/drivers/007/location?lat=21.028511&lng=105.804817"
```

## Kết quả mong đợi

Một lần gửi → **4 dòng log**: 1 của producer và 3 của 3 consumer group, cùng partition/offset:

```
[PRODUCER] đã gửi 1 message  →  topic=driver_gps partition=2 offset=0 | {"driverId":"007",...}
[MAP-SERVICE]           vẽ vị trí lên bản đồ      | partition=2 offset=0 | {"driverId":"007",...}
[NOTIFICATION-SERVICE]  kiểm tra & bắn notification | partition=2 offset=0 | {"driverId":"007",...}
[WORKING-HOURS-SERVICE] cộng dồn giờ làm việc     | partition=2 offset=0 | {"driverId":"007",...}
```

Cùng `partition` + cùng `offset` = **cùng MỘT message trên log**, được 3 group đọc độc lập —
không ai "lấy mất" của ai.

## Thí nghiệm thêm

- Gửi nhiều message cùng `driverId` → luôn vào **cùng partition** (key = driverId, xem Buổi 3 — message key).
- Đổi `groupId` của 2 listener về **cùng một giá trị** → 2 consumer đó bắt đầu **chia nhau** partition,
  mỗi message chỉ còn 1 trong 2 con nhận (đây chính là mô hình Queue — Buổi 3, slide "Queue và Topic trong Kafka").
- Xem offset từng group:
  `docker exec kafka-demo /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --all-groups`

## Dọn dẹp

```bash
docker compose down -v
```
