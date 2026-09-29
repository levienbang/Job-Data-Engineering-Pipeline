# Job Data Engineering Pipeline

## 1. Mục tiêu

Xây một Data Engineering project độc lập để:

- Crawl dữ liệu việc làm từ một website dành cho học web scraping.
- Nạp một dataset việc làm tiếng Việt công khai ở quy mô lớn.
- Lưu raw snapshot, làm sạch, kiểm tra chất lượng và loại bản ghi trùng.
- Tạo dữ liệu dạng bảng ở Parquet.
- Gửi dữ liệu đạt chuẩn vào API `/jobs/import` của AI Career Platform.
- Điều phối pipeline định kỳ và theo dõi từng lần chạy.

Project này không thực hiện embedding, Qdrant, skill taxonomy hay agent. Những phần đó vẫn thuộc AI Career Platform.

---

## 2. Nguồn dữ liệu được chọn

### Nguồn A — Fake Jobs: nguồn crawl chính

- URL: <https://realpython.github.io/fake-jobs/>
- Mục đích: học HTTP request, parse HTML, theo link chi tiết, rate limiting, retry và lưu raw HTML.
- Trang tự mô tả là **“Fake Jobs for Your Web Scraping Journey”**, nên phù hợp hơn để luyện crawler so với website tuyển dụng thương mại.
- Dữ liệu là giả và bằng tiếng Anh; không dùng nguồn này để đánh giá thị trường lao động Việt Nam.

Adapter dự kiến: `FakeJobsCrawler`.

### Nguồn B — VietJobs: dữ liệu tiếng Việt chính

- Dataset: <https://huggingface.co/datasets/dinhieufam/VietJobs>
- Repository: <https://github.com/VinNLP/VietJobs>
- Cách lấy: download dataset đã công bố từ Hugging Face, không crawl lại TopCV.
- Mục đích: chạy pipeline ở quy mô lớn và cung cấp job description tiếng Việt cho AI Career Platform.

Repository công bố dataset gồm 48.092 tin tuyển dụng và hướng dẫn tải bằng:

```bash
huggingface-cli download dinhieufam/VietJobs
```

Adapter dự kiến: `VietJobsDatasetSource`.

### Nguồn không dùng trong MVP

Không crawl trực tiếp ITviec, VietnamWorks, TopCV hoặc VietJobs.vn trong phiên bản đầu. Chỉ thêm một nguồn live khi có API/data feed chính thức, văn bản cho phép, hoặc điều khoản sử dụng đã được kiểm tra và cho phép mục đích dự án.

Không bypass đăng nhập, CAPTCHA, rate limit hoặc biện pháp chống bot.

---

## 3. Tính thực tế của hai nguồn

Hai nguồn mô phỏng hai kiểu ingestion phổ biến trong Data Engineering:

| Source | Kiểu ingest | Tình huống thực tế mô phỏng |
|---|---|---|
| VietJobs | Bulk file ingestion | Historical backfill từ CSV/JSON/Parquet trên object storage |
| Fake Jobs | Incremental web ingestion | Poll nguồn định kỳ, phát hiện record mới/thay đổi |

DE thực tế không nhất thiết phải crawl. Source có thể là file batch, API phân trang, database/CDC hoặc event stream. Giá trị của project nằm ở việc hợp nhất các schema khác nhau thành cùng contract, xử lý lỗi và chạy lại an toàn.

---

## 4. Kiến trúc

```text
Fake Jobs HTML                 VietJobs dataset
      |                              |
FakeJobsCrawler               VietJobsDatasetSource
      |                              |
      +---------- Raw records -------+
                       |
                Bronze Parquet
                       |
             Clean + standardize
                       |
          Validate + quarantine
                       |
                Silver Parquet
                       |
             Deduplicate + map
                       |
                 Gold Parquet
                  /           \
       DuckDB quality checks   DeliveryMapper
                                      |
                         multipart JSON file
                                      |
                            POST /jobs/import
                                      |
                         AI Platform PostgreSQL
                                      |
                                  Qdrant index
```

### Trách nhiệm từng nơi

| Thành phần | Trách nhiệm |
|---|---|
| DE project | Crawl/download, snapshot, clean, validate, deduplicate, export và delivery |
| PostgreSQL của DE | Metadata lần chạy, file đã xử lý, checkpoint và trạng thái delivery |
| AI Platform | LLM extraction, chuẩn hóa skill, business dedup, lưu job và search |
| Qdrant | Vector index được tạo từ dữ liệu đã có trong PostgreSQL của AI Platform |

DE project không ghi trực tiếp vào Qdrant và không import ORM model từ AI Platform.

---

## 5. Stack công nghệ

### Core

| Công nghệ | Dùng để làm gì |
|---|---|
| Python 3.12 | Viết pipeline |
| HTTPX | HTTP request, timeout và retry |
| Beautiful Soup | Parse HTML của Fake Jobs |
| Polars | Làm sạch và biến đổi dữ liệu dạng bảng |
| Parquet | Lưu Bronze, Silver và Gold |
| DuckDB | Truy vấn Parquet để kiểm tra và phân tích |
| Pydantic | Kiểm tra từng record và data contract |
| PostgreSQL + SQLAlchemy | Lưu metadata/checkpoint của pipeline |
| Pytest | Unit và integration tests |
| Docker Compose | Chạy môi trường local đồng nhất |

### Thêm sau core

- Airflow: orchestration, schedule, dependency, retry và backfill.
- AWS S3 + Athena: cloud storage và query Parquet.
- Kafka: chỉ làm lab streaming sau khi batch pipeline hoàn chỉnh.
- Snowflake: chỉ làm lab warehouse nếu còn thời gian.

DuckDB không thay PostgreSQL. DuckDB chủ yếu truy vấn file Parquet; PostgreSQL lưu trạng thái vận hành và phục vụ ứng dụng.

---

## 6. Data layers

### Bronze

- Lưu dữ liệu gần nguyên bản nhất.
- Fake Jobs: lưu raw HTML và record vừa parse.
- VietJobs: lưu snapshot file tải về cùng checksum.
- Không sửa hoặc overwrite snapshot cũ.

Ví dụ:

```text
data/bronze/source=fake_jobs/ingestion_date=YYYY-MM-DD/jobs.parquet
data/bronze/source=vietjobs/ingestion_date=YYYY-MM-DD/jobs.parquet
data/raw_html/fake_jobs/YYYY-MM-DD/<page_hash>.html
```

### Silver

- Chuẩn hóa tên cột và datatype.
- Xóa HTML thừa, chuẩn hóa Unicode và khoảng trắng.
- Parse ngày tháng nhưng không tự bịa field bị thiếu.
- Giữ raw value để truy vết.
- Record lỗi được đưa sang quarantine.

### Gold

- Một hàng tương ứng một tin tuyển dụng.
- Tuân theo canonical contract nội bộ của DE và giữ các field lineage.
- Đã deduplicate ở cấp source.
- Không gửi nguyên record Gold vào `/jobs/import`.
- `DeliveryMapper` đổi Gold record thành import payload của AI Platform.

---

## 7. DE Gold contract v1

Schema cuối cùng chỉ được chốt sau khi inspect file VietJobs thực tế. Contract tối thiểu:

```json
{
  "source_name": "fake_jobs | vietjobs",
  "source_job_id": "string-or-null",
  "source_url": "string-or-null",
  "raw_title": "string",
  "raw_company": "string-or-null",
  "raw_location": "string-or-null",
  "raw_employment_type": "string-or-null",
  "raw_description": "string",
  "posted_at": "datetime-or-null",
  "collected_at": "datetime",
  "source_record_hash": "sha256",
  "schema_version": 1
}
```

Quy tắc:

- Dùng `null` cho dữ liệu không tồn tại, không tự bịa giá trị.
- Mọi record phải có `source_name`, `collected_at` và `source_record_hash`.
- Hash không chứa `collected_at`, để chạy lại vẫn nhận ra cùng record.
- Giữ nguyên text gốc trước khi AI Platform thực hiện LLM extraction.
- Không tự suy diễn required/preferred skill trong DE pipeline.
- `source_name`, `source_job_id`, `posted_at`, `collected_at` và
  `source_record_hash` vẫn được giữ trong Gold/metadata DE dù hiện chưa được
  AI Platform lưu qua import.

---

## 8. Crawler requirements

`FakeJobsCrawler` phải:

1. Chỉ truy cập domain được allowlist.
2. Có timeout và user-agent mô tả project.
3. Giới hạn tốc độ, mặc định tối đa 1 request/giây.
4. Retry lỗi mạng tạm thời bằng exponential backoff và có giới hạn.
5. Không retry vô hạn với lỗi 4xx.
6. Lưu URL, thời điểm lấy, HTTP status và content hash.
7. Lưu raw HTML trước khi parse.
8. Parse trang danh sách và trang chi tiết thành record có cấu trúc.
9. Chạy lại cùng trang không tạo record trùng.
10. Có fixture HTML để test mà không gọi Internet.

Crawler không chứa logic của AI Platform như gọi LLM hoặc chuẩn hóa skill alias.

---

## 9. Data quality và quarantine

Rule tối thiểu:

- `raw_title` và `raw_description` không rỗng.
- `source_name` thuộc allowlist.
- `source_url`, nếu có, phải hợp lệ.
- `posted_at` không nằm bất hợp lý trong tương lai.
- `source_record_hash` đúng định dạng SHA-256.
- Không trùng `(source_name, source_job_id)` khi có source ID.
- Không trùng hash trong cùng batch.

```text
valid record   → Silver/Gold
invalid record → quarantine + error_code + error_detail
```

Một record lỗi không được làm thất bại toàn bộ batch.

---

## 10. PostgreSQL metadata

### `pipeline_runs`

```text
run_id, pipeline_name, source_name, started_at, finished_at,
status, input_count, valid_count, invalid_count, duplicate_count,
delivered_count, error_message, code_version
```

### `source_assets`

```text
asset_id, source_name, source_url_or_path, content_hash, http_status,
first_seen_at, last_seen_at, last_run_id, status
```

### `delivery_batches`

```text
batch_id, run_id, payload_hash, record_count, attempt_count,
status, last_error, delivered_at
```

Các bảng này chỉ lưu operational metadata. Dữ liệu job phục vụ ứng dụng nằm trong PostgreSQL của AI Platform sau khi import.

---

## 11. Delivery tới AI Career Platform

```text
Gold Parquet
→ DeliveryMapper
→ chia thành file JSON theo giới hạn byte
→ multipart field `file`
→ POST /jobs/import
→ lưu kết quả delivery/checkpoint
```

### Mapping contract

| Gold field | Import payload field |
|---|---|
| `raw_title` | `title` |
| `raw_company` | `company` |
| `raw_location` | `location` |
| `raw_employment_type` | `employment_type` |
| `raw_description` | `description` |
| `source_url` | `source_url` |

Payload gửi đi có dạng danh sách JSON hoặc `{"jobs": [...]}`:

```json
[
  {
    "title": "AI Intern",
    "company": "Example",
    "location": "Ho Chi Minh City",
    "employment_type": "Internship",
    "description": "Learn Python...",
    "source_url": "https://example.com/jobs/123"
  }
]
```

Các field lineage chưa được endpoint nhận không bị xóa khỏi Gold. Chúng vẫn
được lưu trong DE project để audit và có thể bổ sung vào AI Platform bằng một
migration riêng sau này.

Yêu cầu:

- Chia batch theo kích thước JSON đã serialize, không chỉ theo số record.
- Mỗi file phải nhỏ hơn giới hạn upload của AI Platform; giới hạn hiện tại được
  báo cáo là 1 MB và phải được xác nhận bằng integration test.
- Timeout và retry có giới hạn.
- Schema version và payload hash được lưu trong delivery metadata; không tự
  thêm field mà endpoint hiện tại không hỗ trợ.
- API unavailable không làm mất batch; batch được đánh dấu để retry.
- Test mặc định dùng fake server, không gọi LLM trả phí.

AI Platform tiếp tục xử lý:

```text
raw job
→ LLM structured extraction
→ skill normalization
→ validation + business dedup
→ PostgreSQL
→ embedding
→ Qdrant
```

---

## 12. Roadmap

### Milestone 1 — Foundation và source inspection

- Khởi tạo repository và configuration.
- Tạo `SourceAdapter` protocol.
- Download một subset VietJobs và inspect schema/null/duplicate.
- Chốt contract v1 sau khi inspect dữ liệu thật.
- Thêm fixtures nhỏ vào tests; không commit dataset đầy đủ.

**Hoàn thành khi:** một subset VietJobs được map thành canonical record.

### Milestone 2 — Web crawler

- Viết `FakeJobsCrawler` bằng HTTPX + Beautiful Soup.
- Crawl list page và detail page.
- Lưu raw HTML, request metadata và parsed records.
- Thêm rate limit, retry, allowlist và content hash.
- Test hoàn toàn bằng HTML fixtures.

**Hoàn thành khi:** chạy crawler hai lần không tạo raw record trùng.

### Milestone 3 — Bronze, Silver và Gold

- Ghi Bronze Parquet cho cả hai source.
- Clean/transform bằng Polars.
- Tạo Silver và Gold Parquet.
- Viết DuckDB query kiểm tra count, null, duplicate và phân bố.

**Hoàn thành khi:** cả hai source đi qua cùng pipeline sau lớp adapter.

### Milestone 4 — Validation và idempotency

- Pydantic validation và quarantine.
- Source-level deduplication.
- File checksum và run checkpoint.
- Test rerun, corrupt file và schema drift.

**Hoàn thành khi:** chạy lại an toàn và record lỗi không làm hỏng batch.

### Milestone 5 — PostgreSQL metadata và delivery

- Alembic migrations cho metadata DB.
- Ghi pipeline run, source asset và delivery batch.
- Dùng `DeliveryMapper`, export JSON chunks và gọi `/jobs/import` bằng
  multipart field `file`.
- Test fake API; live integration là bước riêng có kiểm soát chi phí.

**Hoàn thành khi:** Gold batch được gửi thành công và có checkpoint.

### Milestone 6 — Airflow

```text
acquire → bronze → validate_clean → silver
→ deduplicate_gold → quality_check → deliver
```

- Business logic nằm trong module Python, không viết dồn vào DAG.
- Có schedule, timeout, retry, backfill và failure logging.

**Hoàn thành khi:** DAG chạy end-to-end và retry được task thất bại.

### Milestone 7 — Portfolio hardening

- Docker Compose verification.
- Failure injection: timeout, corrupt HTML/file, duplicate batch và API unavailable.
- Metrics: counts, pass rate, duration, throughput và delivery success.
- Architecture diagram, lineage, README và demo.

**Hoàn thành khi:** người khác clone repo và chạy được demo bằng fixtures/subset.

### Optional Milestone 8 — AWS

- Upload Bronze/Silver/Gold Parquet lên S3.
- Query bằng Athena.
- IAM least privilege, budget alert và cleanup.

### Optional Milestone 9 — Kafka hoặc Snowflake

Chọn một, không cần làm cả hai:

- Kafka để học event streaming, offset, consumer group, replay và idempotent consumer.
- Snowflake để học warehouse, stage, `COPY INTO`, dbt và compute cost.

---

## 13. Repository structure

```text
job-data-pipeline/
├── docs/PROJECT_SPEC.md
├── src/job_pipeline/
│   ├── config.py
│   ├── contracts.py
│   ├── sources/
│   │   ├── base.py
│   │   ├── fake_jobs.py
│   │   └── vietjobs.py
│   ├── bronze.py
│   ├── clean.py
│   ├── validate.py
│   ├── deduplicate.py
│   ├── storage.py
│   ├── delivery.py
│   └── metrics.py
├── dags/job_pipeline.py
├── sql/quality_checks.sql
├── tests/
│   ├── fixtures/
│   ├── unit/
│   └── integration/
├── data/.gitkeep
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

Không commit dataset đầy đủ, Parquet output, raw HTML số lượng lớn, `.env` hoặc credentials.

---

## 14. Tests bắt buộc

- Parse Fake Jobs list/detail từ fixture.
- Timeout, retry limit và domain allowlist.
- VietJobs source mapping.
- Bronze/Silver/Gold Parquet round trip.
- Unicode và HTML cleaning.
- Contract validation và quarantine.
- Dedup trong batch và giữa hai lần chạy.
- DuckDB quality queries.
- PostgreSQL metadata transaction.
- Gold-to-import field mapping.
- Chia file theo giới hạn byte và từ chối một record đơn lẻ vượt giới hạn.
- API delivery success, timeout và retry.
- Airflow DAG import/dependency test.

End-to-end:

```text
HTML/CSV fixture
→ Bronze
→ Silver
→ Gold
→ fake /jobs/import
→ rerun
→ không duplicate delivery
```

---

## 15. Tiêu chí hoàn thành

- Có crawler thật trên Fake Jobs và tests không phụ thuộc live network.
- Có adapter tải/đọc VietJobs public dataset.
- Có Bronze, Silver và Gold dạng Parquet.
- DuckDB tạo được quality report.
- Có quarantine và schema-drift detection.
- Pipeline idempotent khi chạy lại.
- PostgreSQL lưu run/checkpoint/delivery metadata.
- Gold data gửi được tới AI Platform qua API.
- Airflow orchestration chạy được.
- Docker Compose, tests, lint và README đều pass.

AWS, Kafka và Snowflake không phải điều kiện hoàn thành core.

---

## 16. Thứ tự triển khai

```text
Inspect VietJobs + chốt contract
→ Source adapters
→ Fake Jobs crawler
→ Bronze/Silver/Gold
→ Validation + dedup
→ PostgreSQL metadata
→ API delivery
→ Airflow
→ Portfolio hardening
→ AWS/Kafka/Snowflake nếu còn thời gian
```

Điểm quan trọng nhất không phải số lượng công nghệ, mà là chứng minh dữ liệu đi từ nhiều loại nguồn đến AI Platform một cách tái lập, có kiểm tra, không trùng và có thể điều tra khi lỗi.
