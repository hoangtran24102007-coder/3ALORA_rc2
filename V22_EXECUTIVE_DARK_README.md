# 3 Alora V22 · Executive Dark Luxury · Preview 3

Đã triển khai trực tiếp vào mã nguồn trên nền Design Preview 2. Đây là **bản nghiệm thu staging**, chưa phải bản đã cài lên production và chưa đạt nghiệm thu trên thiết bị vật lý.

## Bắt đầu trên Windows

1. Cài Node.js **24.x**. Giải nén vào thư mục mới, ví dụ `C:\3ALORA\previews\V22_LUXURY\3ALORA_ENTERPRISE`.
2. Mở CMD trong thư mục đó, chạy `npm.cmd ci --no-audit --no-fund` rồi `npm.cmd test`.
3. Mở `START_V22_LUXURY_STAGING.bat`. Trình chạy tạo dữ liệu riêng dưới `%LOCALAPPDATA%\3Alora\V22LuxuryStaging`, cổng **3013**, chỉ localhost; không dùng config/database production.
4. Mở `http://127.0.0.1:3013`. Tệp đăng nhập khởi tạo nằm trong thư mục staging và được in đường dẫn trên cửa sổ chạy. Không gửi tệp đó qua chat. Đổi mật khẩu khi ứng dụng yêu cầu.
5. Xem `docs/V22_LUXURY_WINDOWS.md` và `docs/V22_LUXURY_UPGRADE_ROLLBACK.md` trước khi đưa dữ liệu sao lưu vào môi trường nghiệm thu.

## Giao diện đã triển khai

- Bộ token đúng bảng màu Executive Dark, Inter self-hosted kèm SIL OFL, icon SVG nét thống nhất; cùng palette cho công cụ nhúng/cửa vào cũ.
- Đăng nhập desktop/mobile, xem/ẩn mật khẩu, Caps Lock, khóa nút submit khi xử lý, ghi nhớ tên đăng nhập tùy chọn, trợ giúp an toàn.
- Sidebar thu gọn, tìm module/đơn bằng Ctrl/⌘ K, quick actions, chọn Team/ACC được cấp, notification center và điều hướng dưới trên điện thoại.
- Dashboard dùng API thật, KPI/biểu đồ/ưu tiên theo quyền. KPI đơn dùng cùng kỳ thời gian với báo cáo tài chính.
- Bảng đơn có đối tác, tìm SKU/tracking; drawer chi tiết có ảnh design và thời gian tệp; quét vẫn giữ khóa thao tác và operation ID.
- Camera native khi trình duyệt hỗ trợ BarcodeDetector; có hướng dẫn USB/Bluetooth/nhập tay khi không hỗ trợ. Ảnh design xuất hiện trong kết quả tra/quét.
- Chat có vùng cuộn độc lập, header/composer cố định theo bố cục, danh sách/chuyển màn hình mobile; giữ nháp văn bản trong RAM theo người/room và xóa khi hết phiên/đăng xuất.
- Sáng/tối, hỗ trợ tài khoản, đăng ký/tắt push trên thiết bị hiện tại, icon PWA mới và tên ngắn **3 Alora**.

## Tài liệu bàn giao

- `artifacts/v22-luxury/index.html`: gallery ảnh chụp giao diện đang chạy; dữ liệu QA cách ly, không phải số liệu doanh nghiệp.
- `docs/V22_LUXURY_SOURCE_AUDIT.md`: cấu trúc và phạm vi thay đổi có kiểm soát.
- `docs/V22_LUXURY_DESIGN_SYSTEM.md`: token, component, motion, accessibility.
- `docs/V22_LUXURY_CHANGED_FILES.md`: danh sách tệp mới/thay đổi.
- `docs/V22_LUXURY_QA.md`: kết quả kiểm thử đã chạy và giới hạn xác minh.
- `docs/V22_LUXURY_ACCEPTANCE.md`: checklist nghiệm thu và phần còn thiếu.

Runtime vẫn nhận diện **21.0.0-rc3**, schema **31**. V22 là định danh giao diện của gói này. Không ghi đè V21 RC2/V21 RC3 đang chạy, không đổi domain `ops.3alora.io.vn` trong giai đoạn thử.
