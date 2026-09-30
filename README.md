# Job Data Engineering Pipeline

Repo độc lập để thu thập và tiền xử lý dữ liệu việc làm trước khi gửi sang AI Career Platform qua API `/jobs/import`. AI Career Platform tiếp tục xử lý và lưu dữ liệu vào database của hệ thống đó.

## Luồng dữ liệu dự kiến

```text
Fake Jobs / VietJobs
  → Bronze (raw snapshot)
  → Silver (clean + validate)
  → Gold (deduplicate + canonical data)
  → DeliveryMapper → JSON export + manifest
  → POST /jobs/import
  → AI Career Platform
```

Nguồn dữ liệu ban đầu là trang [Fake Jobs](https://realpython.github.io/fake-jobs/) để thực hành crawling và bộ dữ liệu [VietJobs](https://huggingface.co/datasets/dinhieufam/VietJobs) đã công bố. Repo không ghi trực tiếp vào database của AI Career Platform.

## Trạng thái

Hai nguồn VietJobs CSV và Fake Jobs HTML tạo cùng kiểu `CanonicalJob` v2. Gold giữ riêng `raw_description`, requirements, experience, technical/soft skills, qualifications, languages, benefits, salary, working hours, category và country. VietJobs không có company, URL và ngày đăng nên các field đó là `None`. `DeliveryMapper` chỉ ghép lại mô tả có nhãn khi tạo payload; giá trị Gold vẫn giữ cấu trúc gốc. Fake Jobs lưu HTML và metadata trong `data/raw_html/fake_jobs/`.

Milestone 3–4 chạy độc lập: Bronze Parquet lưu JSON của từng source row và lineage, Silver chuẩn hóa Unicode NFC/khoảng trắng và kiểm tra Pydantic, Gold giữ canonical job đã loại trùng. VietJobs được lọc IT sau Bronze. Bản ghi lỗi vào `data/quarantine/` với `error_code` và `error_detail`; record ngoài phạm vi IT chỉ được đếm, không phải lỗi. Bronze có tên theo content hash và không bị ghi đè. Gold hợp nhất theo source ID nếu có, nếu không theo source content hash; timestamp thu thập không tham gia hash. `PipelineResult` có checksum Bronze, checksum CSV input khi dùng VietJobs, đường dẫn và số lượng mỗi nhóm.

Operational metadata gồm `pipeline_runs`, `source_assets`, `delivery_batches` và `delivery_records`. Export tạo JSON batch cùng `manifest.json` trước khi gửi HTTP; file mặc định **nhỏ hơn 1.000.000 byte** và tối đa 20 record. Response thành công ghi từng record là `accepted` hoặc `rejected`; record bị từ chối chỉ gửi lại khi chọn `--retry-rejected`. Batch lỗi vẫn retry được. Application job data nằm trong Gold Parquet và được gửi qua multipart `file`.

**Giới hạn VietJobs:** AI Platform hiện yêu cầu company sau extraction và có taxonomy skill nhỏ. CSV không có company. Mặc định record thiếu company được đánh `blocked`; đặt `DELIVERY_MISSING_COMPANY=send_null` để gửi `company: null` và quan sát phản hồi `rejected`. Đây là kết quả được dự kiến cho tới khi AI Platform được chỉnh, không phải lỗi Crawl. Không tạo company giả. Giới hạn upload của server thật chưa được xác nhận bằng integration test; tests mặc định dùng HTTP giả.

```text
data/bronze/source=<source>/ingestion_date=YYYY-MM-DD/jobs-<hash>.parquet
data/silver/source=<source>/ingestion_date=YYYY-MM-DD/jobs-<hash>.parquet
data/quarantine/source=<source>/ingestion_date=YYYY-MM-DD/jobs-<hash>.parquet
data/gold/source=<source>/jobs.parquet
data/export/source=<source>/run_id=<run_id>/batch-0001.json
data/export/source=<source>/run_id=<run_id>/manifest.json
```

Chạy VietJobs từ file CSV local:

```python
from datetime import UTC, datetime
from pathlib import Path
from job_pipeline.pipeline import run_vietjobs

result = run_vietjobs(Path("data/VietJobs.csv"), collected_at=datetime.now(UTC))
print(result)
```

Fake Jobs: truyền `crawler.iter_records()` vào `run_fake_jobs(records, collected_at=...)`; crawler lưu raw HTML trước khi trả parsed record. Kiểm tra Gold bằng `quality_report(result.gold_path)` hoặc chạy các truy vấn trong `sql/quality_checks.sql` bằng DuckDB. File CSV sai schema hoặc không đọc được sẽ dừng trước Bronze; lỗi từng row sẽ vào quarantine và các row khác vẫn tiếp tục.

Kết quả kiểm tra toàn bộ 48.092 dòng VietJobs và giới hạn của dữ liệu nằm trong [ghi chú VietJobs](docs/VIETJOBS_INSPECTION.md). Kiến trúc và các mốc tiếp theo nằm trong [đặc tả dự án](docs/PROJECT_SPEC.md).

## Cài đặt và kiểm tra

```bash
uv sync --python 3.12 --extra dev
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Để chạy và test DAG với Airflow 2:

```bash
uv sync --python 3.12 --extra dev --extra airflow
```

Các lệnh kiểm tra dùng fixtures và HTTP giả, không gọi website hoặc LLM. Lệnh `uv sync` cần mạng nếu máy chưa có dependencies.

## PostgreSQL metadata và Airflow

Tạo PostgreSQL database riêng cho DE, đặt `PIPELINE_DATABASE_URL` như `.env.example`, rồi chạy:

```bash
uv run alembic upgrade head
uv run alembic check
```

Migration không tạo bảng application jobs. Có thể chạy `alembic downgrade base` trong **database thử nghiệm** để kiểm tra rollback. Không chạy downgrade trên metadata production có dữ liệu.

`execute_vietjobs(...)` và `execute_fake_jobs(...)` chạy pipeline, quality check, export rồi delivery. Mỗi POST chứa file `jobs.json` trong multipart field `file`. Timeout mặc định 300 giây; timeout, lỗi mạng, 429 và 5xx tạm thời được thử tối đa ba lần. 413 và các 4xx khác không retry. Response 2xx có record lỗi vẫn làm batch `succeeded`: record lỗi là `rejected`, các record còn lại là `accepted`. Response sai contract làm batch `failed`. Không có exactly-once transaction xuyên hai hệ thống.

DAG [dags/job_pipeline.py](dags/job_pipeline.py) chạy `acquire → bronze → validate_clean → silver → deduplicate_gold → quality_check → export_payload → deliver`. `DELIVERY_ENABLED=false` giữ export nhưng bỏ qua POST, kết thúc run `succeeded` với `delivered_count=0`. Đặt `PIPELINE_SOURCE`, `PIPELINE_DATA_ROOT`, `VIETJOBS_CSV_PATH`, `PIPELINE_DATABASE_URL`, `AI_PLATFORM_IMPORT_URL` và `PIPELINE_SCHEDULE`; mặc định `@daily`. DAG có 2 retries, timeout 30 phút mỗi task, `catchup=True` và `max_active_runs=1`.

## Export và delivery thủ công

```bash
uv run python -m job_pipeline run --source vietjobs --csv data/VietJobs.csv --root data
uv run python -m job_pipeline export --source vietjobs --root data
uv run python -m job_pipeline deliver --source vietjobs --endpoint http://localhost:8000/jobs/import
```

`run` không cần DB. `export` và `deliver` cần `PIPELINE_DATABASE_URL` và migrations đã chạy. `export` chỉ ghi file và manifest; không gọi HTTP. `deliver` dùng manifest mới nhất của source hoặc `--manifest PATH`. `export --retry-rejected` và `--retry-blocked` cho phép đưa lại đúng record đó vào batch. Khi `DELIVERY_MISSING_COMPANY=send_null`, record trước đó bị `blocked` vì thiếu company được tự động export lại, không cần `--retry-blocked` (kể cả trong DAG). Số `delivered_count`/`rejected_count` của một run được đếm từ `delivery_records`, nên chạy lại `deliver` không ghi đè số liệu; `export` qua CLI tự đóng run của nó. CLI in summary JSON, trả exit code khác 0 khi có batch lỗi hoặc cấu hình sai.

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `DELIVERY_PAYLOAD_PROFILE` | `ai_platform_v1` | `ai_platform_v1` hoặc `extended` |
| `DELIVERY_MISSING_COMPANY` | `block` | `block` hoặc `send_null` |
| `DELIVERY_MAX_BYTES` | `1000000` | kích thước file phải nhỏ hơn giá trị này |
| `DELIVERY_MAX_RECORDS` | `20` | tối đa record trong batch |
| `DELIVERY_TIMEOUT_SECONDS` | `300` | timeout mỗi POST |
| `DELIVERY_MAX_ATTEMPTS` | `3` | số lần thử tối đa |
| `DELIVERY_ENABLED` | `true` | `false` để chỉ export trong DAG |
| `PIPELINE_EXPORT_ROOT` | `data/export` | thư mục export |

Profile `ai_platform_v1` gửi đúng sáu key: `title`, `company`, `location`, `employment_type`, `description`, `source_url`. Profile `extended` thêm lineage (`source_name`, `source_record_hash`, `posted_at`) và các field gốc còn lại theo tên không có tiền tố `raw_`; chỉ dùng khi endpoint hỗ trợ. Xem checkpoint bằng `SELECT record_key, status, error, updated_at FROM delivery_records;` trong metadata DB.

## Kiểm tra VietJobs trên máy local

Tải file CSV công bố (lệnh này **dùng mạng**):

```bash
mkdir -p data
curl -L --fail https://huggingface.co/datasets/dinhieufam/VietJobs/resolve/main/VietJobs.csv -o data/VietJobs.csv
```

Xem schema, số dòng, null và số dòng thuộc phạm vi công nghệ (**chỉ đọc file local**):

```bash
uv run python scripts/inspect_vietjobs.py data/VietJobs.csv
```

`VietJobsDatasetSource.iter_rows()` đọc mọi dòng nguồn. Bộ lọc `is_technology_job(row)` được áp dụng sau khi đọc để quyết định dòng nào đi tiếp. Nó giữ category `công_nghệ_thông_tin_kỹ_thuật_số` và một số tiêu đề nghề công nghệ cụ thể ở category khác; không tìm từ khóa chung trong description. Trên toàn bộ file hiện tại, quy tắc giữ 2.043 dòng. Đây là quy tắc xác định phạm vi, chưa phải nhãn đúng tuyệt đối. `source_record_hash` dùng toàn bộ dòng CSV với thứ tự khóa ổn định, nên cùng nội dung có cùng hash khi chạy lại; nó không phải ID bền vững của một tin đã cập nhật.

## Thử Fake Jobs thủ công

Đoạn sau **dùng mạng**, lấy tối đa hai công việc và lưu HTML, metadata, bản ghi đã parse trong `data/`:

```bash
uv run python - <<'PY'
from datetime import UTC, datetime
from itertools import islice

from job_pipeline.sources.fake_jobs import FakeJobsCrawler

with FakeJobsCrawler() as source:
    for job in islice(source.iter_jobs(collected_at=datetime.now(UTC)), 2):
        print(job.source_url, job.raw_title, job.source_record_hash)
PY
```

Không commit dataset, raw dumps, Parquet output, `.env` hoặc credentials. Các file dữ liệu dùng khi chạy pipeline được lưu ở `data/` trên máy local.
