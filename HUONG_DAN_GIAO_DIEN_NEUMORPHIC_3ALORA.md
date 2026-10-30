# 3 ALORA · Neumorphic 3D Login · Bản chỉnh trực tiếp mã nguồn V22

## Điểm mới

- Giao diện trang đăng nhập **sáng, nổi 3D (neumorphism)** theo ảnh tham khảo: thẻ giữa màn hình, khối tròn có chuyển động nhẹ, logo 3 Alora thật, ô nhập lõm, nút Login nổi.
- Hiệu ứng xuất hiện, trôi khối cầu, rê chuột parallax trên desktop, focus/hover/click có phản hồi, tắt tự động nếu thiết bị chọn **Reduce Motion**.
- Tối ưu desktop, iPhone và Android; font Inter từ gói sẵn có, không phụ thuộc CDN.
- Đăng nhập hệ thống chính `/enterprise.html` **và** trang cũ `/index.html` đồng bộ giao diện; giữ nguyên ID form, API đăng nhập, ẩn/hiện mật khẩu, cảnh báo Caps Lock và ghi nhớ **chỉ tên tài khoản** theo lựa chọn.
- Không tự mở đăng ký công khai: tài khoản do quản trị viên cấp; liên kết hỗ trợ trên hệ thống chính mở hướng dẫn có sẵn.
- Backend, database, quyền tài khoản, tự động đồng bộ, dashboard/chat/orders và domain: **không thay đổi** trong bản thiết kế này.

## Xem thử ngay không cần chạy server

1. Giải nén ZIP.
2. Mở thư mục `3ALORA_ENTERPRISE`.
3. Nhấp đúp `XEM_TRUOC_GIAO_DIEN_LOGIN_3ALORA.html` bằng Chrome/Edge.
4. Thử rê chuột, bấm hiện/ẩn mật khẩu, điền ô nhập. Nút **Login** trong HTML xem thử **không xác thực**, không gửi bất kỳ dữ liệu nào tới server.

## Khởi chạy nguyên bản thật trên Windows (staging an toàn)

1. Cài **Node.js 24.x** (không dùng bản 22). Không đè lên thư mục đang chạy chính thức.
2. Giải nén ZIP vào thư mục **mới**, ví dụ `C:\3ALORA\NEU_TEST\3ALORA_ENTERPRISE`.
3. Tại thanh địa chỉ của Windows Explorer trong thư mục này, gõ `cmd` rồi Enter. Chạy:

```bat
node -v
npm.cmd ci --no-audit --no-fund
```

4. Sau khi cài dependency thành công, nhấp đúp `START_V22_LUXURY_STAGING.bat`.
5. Mở Chrome / Edge tại **http://127.0.0.1:3013**. Trang chính thực tế nằm ở `http://127.0.0.1:3013/enterprise.html`.
6. Khi khởi động staging lần đầu, cửa sổ lệnh sẽ in đường dẫn đến `STAGING_LOGIN.json` của riêng máy. Mở tệp này trên máy để lấy tài khoản thử, đổi mật khẩu theo yêu cầu. **Không gửi tệp đó cho người khác.**
7. Dừng staging bằng `Ctrl+C` trong cửa sổ của nó.

### Lưu ý về dữ liệu & domain

- Chạy staging sử dụng database riêng tại `%LOCALAPPDATA%\3Alora\V22LuxuryStaging`, `127.0.0.1:3013`, **không phải** dữ liệu thật và **không đổi domain**.
- Domain `ops.3alora.io.vn` vẫn trỏ vào hệ thống đang chạy. Gói ZIP không thể cập nhật domain từ xa. Muốn dùng giao diện mới trên domain, cần sao lưu DB/config, nghiệm thu staging và thực hiện triển khai có rollback vào đúng server.
- Không chạy các script `INSTALL_SERVICE`, `RESTORE` hoặc copy file mới vào production khi chưa sao lưu và chốt kế hoạch nâng cấp.

## Tệp đã thay đổi

- `public/enterprise.html`: trang đăng nhập chính; giữ nguyên dashboard khi đã xác thực.
- `public/index.html`: trang đăng nhập cũ; giữ nguyên ứng dụng cũ khi đăng nhập.
- `public/v23-neumorphic-login.css`: toàn bộ thiết kế mới, phạm vi giới hạn chỉ phần đăng nhập.
- `public/v23-neumorphic-motion.js`: chuyển động tương tác và ghi nhớ tên tài khoản của trang cũ (chỉ chọn ghi nhớ tên).
- `XEM_TRUOC_GIAO_DIEN_LOGIN_3ALORA.html`: xem thử giao diện offline, không chứa đăng nhập thật.
- `tests/neumorphic-login.test.js`: bộ kiểm tra giữ nguyên hợp đồng dữ liệu/đăng nhập.

## Kiểm thử & giới hạn

- Đã kiểm tra cú pháp JS, hợp đồng UI V22, hành vi đăng nhập có bảo vệ double-submit và launcher staging. Chưa kiểm thử trình duyệt thực tế trên máy Windows/iPhone/Android hoặc triển khai production.
- Việc `npm test` đầy đủ cần có dependencies cài bằng `npm ci`; khi thiếu `web-push` hoặc `sharp`, một số bài test sẽ không chạy.
- Node.js runtime/server giữ phiên bản `21.0.0-rc3` và schema 31; phiên bản V22/V23 ở đây **chỉ là phiên bản thiết kế giao diện**.
