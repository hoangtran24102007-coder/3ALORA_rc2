# V21.0.0-rc3

- Thêm Workspace Enterprise thống nhất và các service riêng cho quyền, đơn, danh mục, dự toán, tài chính, analytics, chat và tiếp nhận dữ liệu cũ.
- Thêm migration 31 với snapshot Team, grants theo Account/Team, kỳ tài chính và lịch sử.
- Bổ sung template import XLSX có kiểm tra duyệt/hash, đọc đúng số dòng Excel kể cả dòng trống; không chạy công thức của file nguồn.
- Thêm giá theo số lượng/ngày và phí ship vượt mốc/phụ phí; tính kiện nhiều SKU, hệ số và làm tròn theo dịch vụ.
- Bổ sung connector Google riêng giữ parser Etsy gốc; xử lý đúng Deposit PHP và tiền nhỏ, không coi Sales tax/Payout là sale.
- Thêm ảnh sản phẩm, PDF Unicode phía server, màn hình lịch sử chat cũ theo quyền và outbox push retry.
- Thêm bổ sung thuộc tính giá/tính lại Cost có xác nhận, bắt hoàn thiện hồ sơ người dùng mới, và biểu đồ Team/Account/SKU theo dữ liệu thật.
- Thêm report/closing thực tế, NULL so với 0, đối soát Payout và số liệu snapshot.
- Giữ ID/dòng khi nhận đơn cũ; chặn API cũ sửa đơn đã chuyển vào Workspace để giữ quyền và khóa sổ.
- Thêm Windows service wrapper từ mã nguồn, script vận hành và restore sang thư mục mới.
- Cập nhật kiểm tra version/schema/cache tag đã lỗi thời trong test cũ. Không bỏ kiểm tra nghiệp vụ để làm xanh kết quả.
- Đã sửa lỗi ẩn login và fallback mã request trong môi trường browser QA không có crypto.randomUUID.

Xem `docs/DELIVERY_SCOPE.md` về những phần chưa nghiệm thu và chức năng chưa hoàn tất toàn bộ prompt.

## Chốt bản đóng gói

- Giữ snapshot Cost/Ship của dự toán khi chuyển thành đơn; retry không tạo trùng.
- Cho sửa thông tin đơn có revision/lý do; giữ quyền sở hữu dữ liệu khách/địa chỉ khi nguồn đồng bộ lại.
- Tách ghi chú nội bộ khỏi dữ liệu Partner được đọc.
- Giữ đủ nội dung cá nhân hóa nhiều dòng từ Sheet và nhập thủ công.
- Giá trống không được coi là giá 0; 0 phải được nhập xác nhận rõ ràng.

## RC2 — hoàn thiện vận hành

- Chuẩn bị và diễn tập nâng cấp từ backup đã xác minh trên bản sao, giữ server thật đang chạy; lưu báo cáo kiểm tra khóa chính/số bản ghi/integrity.
- Service dừng Node qua stdin để chạy graceful shutdown và backup, không Kill ngay khi Stop.
- Python/PDF dùng runtime package riêng trong dữ liệu; lỗi spawn/pip dừng cài đặt, không báo thành công giả; chặn Python cài theo user khi cài LocalService.
- Kiểm tra health/domain phải khớp version/schema, chờ readiness thay cho chờ cứng 3 giây.
- Tạo VAPID trực tiếp trong config riêng, giữ khóa đã có; thống kê thiết bị và gửi thử cho chính tài khoản từ Workspace.

## RC3 — khôi phục quét mã vạch

Khôi phục Tra đơn nhanh và Quét quy trình trong menu, thêm ô quét tại trang Sản xuất/Đơn hàng. Tái sử dụng service sản xuất/QC/đóng gói/xuất cũ, có kiểm tra Team và đồng bộ trạng thái Enterprise. Tra cứu chỉ đọc; retry giữ operation ID, đối chiếu revision, xác nhận QC/đóng gói/xuất, xử lý mã trùng Account và chặn quét lặp. Không có migration schema mới.
