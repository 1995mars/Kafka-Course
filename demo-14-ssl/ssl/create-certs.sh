#!/usr/bin/env bash
# Sinh toàn bộ chứng chỉ cho demo SSL — chạy MỘT lần trước docker compose up.
# Chỉ cần keytool (có sẵn trong JDK) — chạy được trong Git Bash / WSL / Linux / macOS.
#
# Năm bước dưới đây khớp 1:1 với slide "Các bước setup SSL cho Kafka" (Buổi 14).
# Slide dùng openssl cho bước CA/ký; ở đây làm trọn bằng keytool vì hai lý do:
#   - không phụ thuộc openssl trên Windows (bản trong Git Bash không đọc được /dev/fd),
#   - keytool cho backdate (-startdate -1d): VM của Docker Desktop hay lệch đồng hồ
#     vài chục giây so với host, cert "mới ra lò" sẽ dính CertificateNotYetValidException.
set -euo pipefail

# Git Bash trên Windows hay "dịch" /CN=... thành đường dẫn C:/... — tắt trò đó đi
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")"
PASS=motdevdemo          # demo thôi — thật thì mỗi store một password và không hardcode
VALIDITY=3650
SAN="SAN=DNS:localhost,DNS:kafka"
mkdir -p secrets
cd secrets
rm -f -- *.jks *.crt *.csr broker_creds client-ssl.properties

echo "=== Bước 1: tạo Certificate Authority (CA) — bên thứ ba đáng tin cậy ==="
# bc:c = BasicConstraints CA:true — thiếu là PKIX từ chối nhận đây là CA
keytool -genkeypair -keystore ca.keystore.jks -alias ca \
    -validity $VALIDITY -startdate -1d -keyalg RSA -storepass "$PASS" -keypass "$PASS" \
    -dname "CN=MotDev-Demo-CA" -ext bc:c
keytool -exportcert -keystore ca.keystore.jks -alias ca -file ca.crt -storepass "$PASS"

echo "=== Bước 2: broker tạo keystore (két riêng chứa private key) + certificate request ==="
# SAN phải chứa mọi hostname client sẽ dùng để gọi broker: localhost (từ host)
# và kafka (nếu sau này có client gọi từ container khác trong cùng network)
keytool -genkeypair -keystore kafka.broker.keystore.jks -alias broker \
    -validity $VALIDITY -startdate -1d -keyalg RSA -storepass "$PASS" -keypass "$PASS" \
    -dname "CN=localhost" -ext "$SAN"
keytool -certreq -keystore kafka.broker.keystore.jks -alias broker \
    -file broker.csr -storepass "$PASS"

echo "=== Bước 3: CA ký request, import CA cert + signed cert ngược vào keystore ==="
# phải khai lại SAN lúc ký — extension không tự đi theo CSR, quên là client
# báo "No subject alternative names" khi verify hostname
keytool -gencert -keystore ca.keystore.jks -alias ca \
    -infile broker.csr -outfile broker-signed.crt \
    -validity $VALIDITY -startdate -1d -storepass "$PASS" -ext "$SAN"
# thứ tự import: CA trước (để keytool dựng được chain), rồi mới đến cert đã ký
keytool -importcert -keystore kafka.broker.keystore.jks -alias CARoot \
    -file ca.crt -storepass "$PASS" -noprompt
keytool -importcert -keystore kafka.broker.keystore.jks -alias broker \
    -file broker-signed.crt -storepass "$PASS" -noprompt

echo "=== Bước 4: tạo truststore chứa CA — cho client, và một bản cho broker ==="
keytool -importcert -keystore kafka.client.truststore.jks -alias CARoot \
    -file ca.crt -storepass "$PASS" -noprompt
# broker cũng cần truststore (image apache/kafka đòi đủ bộ khi bật SSL;
# và sẵn sàng cho mutual TLS nếu sau này bật ssl.client.auth=required)
keytool -importcert -keystore kafka.broker.truststore.jks -alias CARoot \
    -file ca.crt -storepass "$PASS" -noprompt
# ảnh apache/kafka đọc password từ file credentials thay vì env trực tiếp
printf "%s" "$PASS" > broker_creds

echo "=== Bước 5: file config cho CLI tools trong container (Spring Boot xem application.yml) ==="
cat > client-ssl.properties <<EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
ssl.truststore.password=$PASS
EOF

echo ""
echo "Xong. Đã sinh trong ssl/secrets/:"
ls -1
