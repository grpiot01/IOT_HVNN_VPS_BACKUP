# IOT_HVNN — Bản sao code đang chạy trên VPS

Repo độc lập với repo phát triển trên PC. Nguồn được lấy trực tiếp từ `C:\backend`, không lấy từ working tree PC.

## Có trong bản sao

- Các module Python mà ứng dụng backend sử dụng, template, tệp khởi động.
- Web đã build trong `backend/static`, gồm các asset hiện có trên VPS.
- Danh sách phiên bản thư viện của môi trường Python đang chạy.
- `.env.example` chỉ có tên biến, không chứa giá trị thật.
- `snapshot-manifest.json`: dấu thời gian, checksum và tên biến đã được loại khỏi source nếu có.

Đây là bản sao triển khai: VPS chỉ có web đã build nên repo không tự bổ sung source React/STM32/ESP32 từ PC. Một số giá trị bí mật nếu xuất hiện trong source được thay bằng placeholder; khôi phục chúng từ kho riêng trước khi chạy lại.

## Giữ ngoài GitHub

`.env`, JWT/broker/Drive/tunnel credentials, database SQLite, dữ liệu CSV, log, bộ đếm nước, các bản firmware đã upload và các gói backup riêng. Repo này không thay thế bản sao lưu đầy đủ dữ liệu và hạ tầng. Không đưa cookie Chrome hoặc thông tin đăng nhập GitHub vào repo.

## Khôi phục code

1. Clone repo vào thư mục mới; dừng/đổi dịch vụ đang chạy chỉ khi đã sẵn sàng triển khai.
2. Cài Python cùng phiên bản chính/phụ ghi trong manifest. Tạo venv và cài `requirements-vps-lock.txt`.
3. Lấy các giá trị cấu hình và database từ bản sao lưu riêng, đặt `.env` cạnh các file Python của backend.
4. Khôi phục các placeholder nêu trong manifest. Chạy `python -m uvicorn main:app --host 127.0.0.1 --port 10000` từ thư mục backend, rồi kiểm tra `/health` và `/`.
5. Thiết lập Scheduled Task `TomatoBackend`, tunnel, MQTT và Drive theo gói khôi phục riêng. Kiểm tra kết nối thiết bị; HTTP 200 không chứng minh thiết bị đã chạy lệnh.

## Cập nhật bản sao

Chạy bằng Python venv trên VPS: `python C:\vps-backups\IOT_HVNN_VPS_BACKUP\tools\backup_vps_code.py --prepare`, sau đó dùng `--push` để commit/push. Mỗi lần chụp sẽ cập nhật manifest. Chưa thiết lập lịch tự động. Công cụ không sửa `C:\backend`, không khởi động lại dịch vụ và không gửi lệnh thiết bị.
