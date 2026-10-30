> **V25 RC1 — PRODUCTION CANDIDATE, NOT APPROVED FOR LIVE DOMAIN.** Start with `V25_BAT_DAU_TAI_DAY.txt` and `docs/V25_RC1_HUONG_DAN_CHUYEN_DOMAIN.md`. The production gate fails closed until all tests and real infrastructure checks pass.

> **V22 Luxury Preview 3:** bắt đầu tại `V22_EXECUTIVE_DARK_README.md`. Dùng `START_V22_LUXURY_STAGING.bat` để thử riêng. Nội dung bên dưới là hướng dẫn nền RC3, không phải yêu cầu cài đè production.

# 3 Alora Enterprise V21 RC3

Mã nguồn nâng cấp từ bản V20.1 RC2 được cung cấp. Có backend Node.js, SQLite thật, giao diện web/PWA, kết nối Apps Script và kiểm thử API. **Đây là bản ứng viên để nghiệm thu triển khai, chưa được xác nhận vận hành trên máy Windows/domain thực tế của chủ hệ thống.**

Đọc [hướng dẫn cài Windows](docs/INSTALL_WINDOWS.md) trước. Không giải nén đè lên thư mục đang chạy, không thay database bằng database rỗng.

| Việc cần làm | Tệp mở trên Windows |
|---|---|
| Chuẩn bị, sao lưu và diễn tập migration trên bản sao | `CHUAN_BI_3ALORA_WINDOWS.bat` |
| Diễn tập lại trên bản sao mới | `DIEN_TAP_NANG_CAP_3ALORA_WINDOWS.bat` |
| Tạo VAPID trong config riêng, giữ khóa đã có | `CAU_HINH_THONG_BAO_3ALORA_WINDOWS.bat` |
| Chỉ cài dependency, kiểm tra mã và sao lưu | `INSTALL_3ALORA_WINDOWS.bat` |
| Chạy máy chủ / khởi động service đã cài | `START_3ALORA_WINDOWS.bat` |
| Dừng / khởi động lại service | `STOP_3ALORA_WINDOWS.bat` / `RESTART_3ALORA_WINDOWS.bat` |
| Cài Windows service tự chạy khi khởi động máy | `INSTALL_SERVICE_3ALORA_WINDOWS.bat` |
| Kiểm tra máy chủ / HTTPS ngoài Internet | `HEALTH_3ALORA_WINDOWS.bat` / `CHECK_DOMAIN_3ALORA_WINDOWS.bat` |
| Sao lưu / phục hồi sang thư mục mới | `BACKUP_3ALORA_WINDOWS.bat` / `RESTORE_3ALORA_WINDOWS.bat` |

Các tệp mang tên V15–V20 là tài liệu và công cụ lịch sử. Bộ khởi động chính của V21 là các tên trong bảng trên. Giao diện mới ở `/` và `/enterprise.html`; Admin còn có thể mở `/index.html` để đối chiếu dữ liệu vận hành cũ.

## Nội dung chính

- Team, Account Etsy, hồ sơ Shop, người quản lý và nhân viên phụ trách.
- Vai trò theo Team/Account; kiểm tra quyền tại API, chat, báo cáo và tải tệp.
- Tạo đơn thủ công, đồng bộ nguồn có Bearer token, chống trùng, trạng thái nguồn và trạng thái xử lý nội bộ.
- Import Cost/Ship từ Excel, CSV hoặc Apps Script; xem trước và duyệt phiên bản.
- Danh mục sản phẩm, thông số kiện, cân thực, cân thể tích và làm tròn theo dịch vụ.
- Dự toán nhiều sản phẩm, quy đổi chính xác, lưu dự toán và chuyển thành đơn một lần.
- Chi phí thực tế, công nợ đối tác, báo cáo Account/Team, đối soát Payout và ảnh chụp kỳ khóa sổ.
- Thống kê dữ liệu thật; phân bổ số liệu SKU được ghi rõ phương pháp.
- Chat nhóm/cá nhân/Team/đơn/đối tác, tag, ghim, thu hồi, xóa phía mình và tệp có kiểm tra quyền.
- Sao lưu nhất quán WAL, phục hồi cách ly, migration bổ sung schema 31.

## Tài liệu

- [Cài đặt Windows](docs/INSTALL_WINDOWS.md)
- [Thao tác hằng ngày](docs/USER_GUIDE.md)
- [Quản trị và quyền](docs/ADMIN_GUIDE.md)
- [Đồng bộ Google](docs/GOOGLE_SYNC_GUIDE.md)
- [Tài chính](docs/FINANCE_GUIDE.md)
- [API](docs/API_DOCUMENTATION.md)
- [Cập nhật, sao lưu và rollback](docs/UPDATE_GUIDE.md)
- [Kiến trúc](docs/ARCHITECTURE_ENTERPRISE.md)
- [Phạm vi bàn giao và phần cần nghiệm thu](docs/DELIVERY_SCOPE.md)
- [Bằng chứng QA](docs/QA_REPORT.md)

## Cho lập trình viên

Runtime đã kiểm tra: Node.js 24.19.0, Python 3.12.14 trên Linux. Cần Node >=22.13 và Python >=3.10 để đọc file Excel bằng thư viện chuẩn Python. `npm ci` cài dependency từ lockfile. `npm run check`, `npm run test:enterprise`, `npm run test:alora` và `npm test` chạy kiểm tra. `npm run dev` chỉ mở môi trường kiểm tra giao diện với database tạm cách ly; không dùng lệnh này cho dữ liệu thật.

Bộ ZIP không chứa database thật, tài khoản đăng nhập thật, khóa Google, token nguồn, VAPID private key hoặc chứng chỉ domain. Không có secret mẫu nào dùng được để đăng nhập máy chủ của anh.
