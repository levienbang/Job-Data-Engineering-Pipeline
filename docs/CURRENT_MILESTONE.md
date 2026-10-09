# Không có milestone đang làm

Đã hoàn thành Milestone 8, 8.1, 8.2 và **8.3 — Arbeitnow API incremental**.

Milestone 8.3 bổ sung:

- Adapter Arbeitnow public API: allowlist, phân trang, rate limit, bounded retry
  và watermark từ Gold trừ 60 phút; Bronze giữ nguyên mọi job.
- Mapper canonical v2, giải mã HTML, lọc tiếng Anh rồi IT sau Bronze;
  schema cho phép key thừa, thiếu key vào quarantine.
- CLI `run --source arbeitnow`, Airflow acquisition snapshot và
  `ARBEITNOW_MAX_PAGES`; không thêm dependency, DB table hay migration.
- Tests fixture, rerun/dedup, CLI/workflow; kiểm tra API thật 1 trang mỗi lần,
  không export/delivery. Gold 46 job lúc Codex kiểm tra; sau khi mở rộng bộ lọc IT, cùng trang cho Gold 82 job.

Xem [báo cáo kiểm tra 8.3](MILESTONE_8_3_VERIFICATION.md) để biết số liệu,
20 tiêu đề Gold, 10 tiêu đề bị lọc và giới hạn. Lần live thứ hai đọc watermark
nhưng dừng do `max_pages=1`; fixture xác nhận dừng do watermark khi cả trang
cũ hơn ngưỡng. Không thể yêu cầu trang có job mới đồng thời dừng vì watermark.

Chờ yêu cầu milestone mới; không tự triển khai lại hay thêm hạ tầng optional.
