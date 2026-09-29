# Job Data Engineering Pipeline

Repo độc lập để thu thập và tiền xử lý dữ liệu việc làm trước khi gửi sang AI Career Platform qua API `/jobs/import`. AI Career Platform tiếp tục xử lý và lưu dữ liệu vào database của hệ thống đó.

## Luồng dữ liệu dự kiến

```text
Fake Jobs / VietJobs
  → Bronze (raw snapshot)
  → Silver (clean + validate)
  → Gold (deduplicate + canonical data)
  → DeliveryMapper
  → POST /jobs/import
  → AI Career Platform
```

Nguồn dữ liệu ban đầu là trang [Fake Jobs](https://realpython.github.io/fake-jobs/) để thực hành crawling và bộ dữ liệu [VietJobs](https://huggingface.co/datasets/dinhieufam/VietJobs) đã công bố. Repo không ghi trực tiếp vào database của AI Career Platform.

## Trạng thái

Repo đang ở bước khởi tạo. Kiến trúc, hợp đồng dữ liệu, các mốc triển khai và tiêu chí kiểm thử nằm trong [đặc tả dự án](docs/PROJECT_SPEC.md). Mốc đầu tiên là kiểm tra schema của VietJobs, rồi chốt data contract và xây dựng source adapter.

Không commit dataset, raw dumps, Parquet output, `.env` hoặc credentials. Các file dữ liệu dùng khi chạy pipeline được lưu ở `data/` trên máy local.
