# Technical reference

> Detailed notes moved from the original README: settings, design decisions and
> per-milestone behaviour. Start with the [project README](../README.md).

Repo độc lập để thu thập và tiền xử lý dữ liệu việc làm trước khi gửi sang AI Career Platform qua API `/jobs/import`. AI Career Platform tiếp tục xử lý và lưu dữ liệu vào database của hệ thống đó.

## Luồng chính

```text
Source.iter_raw() → snapshot_bronze()
  → validate_clean_bronze() [map_<source>_row ở đây]
  → publish_silver() → deduplicate_gold() → quality → export → deliver
  → POST /jobs/import → AI Career Platform
```

| File | Bước và trách nhiệm |
|---|---|
| `sources/base.py`, `sources/vietjobs.py`, `sources/fake_jobs.py`, `sources/arbeitnow.py` | `RawSource.iter_raw()` đọc CSV hoặc HTTP, giữ field nguồn; các mapper là hàm thuần, được gọi sau Bronze. Fake Jobs lưu HTML + metadata để audit. |
| `pipeline.py` | Entry point chạy các stage và bảng `MAPPERS` cho hai nguồn. |
| `stages.py` | Snapshot Bronze bất biến → map, lọc IT, clean/validate/quarantine → Silver → deduplicate Gold. |
| `quality.py` | Kiểm tra chất lượng Gold. |
| `delivery.py` | `DeliveryMapper` chuyển Gold sang import contract, export JSON theo byte và gửi API. |
| `workflow.py`, `__main__.py` | Hai lối vào: các task Airflow (DAG) và CLI chạy tay. |

Thư mục `data/parsed/` từ các phiên bản cũ không còn được dùng; dữ liệu đã có trong đó được giữ nguyên.

Nguồn dữ liệu ban đầu là trang [Fake Jobs](https://realpython.github.io/fake-jobs/) để thực hành crawling và bộ dữ liệu [VietJobs](https://huggingface.co/datasets/dinhieufam/VietJobs) đã công bố. Repo không ghi trực tiếp vào database của AI Career Platform.

## Trạng thái

Ba nguồn VietJobs CSV, Fake Jobs HTML và Arbeitnow API tạo cùng kiểu `CanonicalJob` v2. Gold giữ riêng `raw_description`, requirements, experience, technical/soft skills, qualifications, languages, benefits, salary, working hours, category và country. VietJobs không có company, URL và ngày đăng nên các field đó là `None`. `DeliveryMapper` chỉ ghép lại mô tả có nhãn khi tạo payload; giá trị Gold vẫn giữ cấu trúc gốc. Fake Jobs lưu HTML và metadata trong `data/raw_html/fake_jobs/`.

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

Fake Jobs: truyền `crawler.iter_raw()` vào `run_fake_jobs(records, collected_at=...)`; crawler lưu raw HTML trước khi trả dict thô. Kiểm tra Gold bằng `quality_report(result.gold_path)` (DuckDB trong `quality.py`), hoặc xem kết quả in ra từ `python -m job_pipeline run`. File CSV sai schema hoặc không đọc được sẽ dừng trước Bronze; lỗi từng row sẽ vào quarantine và các row khác vẫn tiếp tục.

Kết quả kiểm tra toàn bộ 48.092 dòng VietJobs và giới hạn của dữ liệu nằm trong [ghi chú VietJobs](VIETJOBS_INSPECTION.md).

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

DAG (hoặc CLI `run` → `export` → `deliver`) chạy pipeline, quality check, export rồi delivery. Mỗi POST chứa file `jobs.json` trong multipart field `file`. Timeout mặc định 300 giây; timeout, lỗi mạng, 429 và 5xx tạm thời được thử tối đa ba lần. 413 và các 4xx khác không retry. Response 2xx có record lỗi vẫn làm batch `succeeded`: record lỗi là `rejected`, các record còn lại là `accepted`. Response sai contract làm batch `failed`. Không có exactly-once transaction xuyên hai hệ thống.

[dags/job_pipeline.py](../dags/job_pipeline.py) tạo **một DAG cho mỗi nguồn** từ cùng 8 task `acquire → bronze → validate_clean → silver → deduplicate_gold → quality_check → export_payload → deliver`: `job_pipeline_vietjobs` (CSV tĩnh, không lịch, chạy tay) và `job_pipeline_arbeitnow` (lịch `ARBEITNOW_SCHEDULE`, mặc định `@daily`). Mỗi DAG truyền nguồn qua `params.source`, nên hai nguồn chạy độc lập; `PIPELINE_SOURCE` chỉ còn là giá trị dự phòng. `DELIVERY_ENABLED=false` giữ export nhưng bỏ qua POST, kết thúc run `succeeded` với `delivered_count=0`. Đặt `PIPELINE_DATA_ROOT`, `VIETJOBS_CSV_PATH`, `PIPELINE_DATABASE_URL`, `AI_PLATFORM_IMPORT_URL`, `ARBEITNOW_MAX_PAGES` và `ARBEITNOW_SCHEDULE`. Mỗi DAG có 2 retries, timeout 30 phút mỗi task, `catchup=False` (API chỉ trả job hiện tại; CSV không đổi theo ngày) và `max_active_runs=1`. DAG mới được tạo ở trạng thái paused.

## Chạy bằng Docker

Docker Compose chạy PostgreSQL 17 và Airflow 2.10.5 / Python 3.12 với
`LocalExecutor`. Image cài project theo constraints có sẵn của Airflow, giữ
SQLAlchemy 1.4.54 và chạy `pip check` khi build. Không copy dataset hay secrets
vào image. PostgreSQL có hai database và role riêng: `job_pipeline` cho DE,
`airflow` cho Airflow; init chạy migrations cho cả hai trước khi khởi động
scheduler và webserver.

| Dịch vụ | Cổng host | Địa chỉ trong container |
|---|---|---|
| Crawl PostgreSQL | `127.0.0.1:5433` | `postgres:5432` |
| Airflow UI | `127.0.0.1:8080` | `airflow:8080` |
| AI Platform (`../LLM`) | `8000` | `host.docker.internal:8000` |

Cổng `5432` của host được giữ cho PostgreSQL của LLM. Compose chỉ dùng database
riêng của Crawl; giao tiếp sang LLM qua HTTP.

1. Copy `.env.example` thành `.env` (git-ignored). Thay mọi giá trị
   `replace-with-...`, dùng mật khẩu DB dạng hex để an toàn trong URL.
   Tạo mỗi secret hex bằng `openssl rand -hex 32`; tạo Fernet key bằng lệnh ghi
   trong `.env.example`. `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD` và
   `AIRFLOW_ADMIN_EMAIL` dùng để tạo tài khoản khi init lần đầu. Đổi `.env`
   sau đó không tự đổi mật khẩu user/database đã tồn tại.
2. Đặt CSV local ở `data/VietJobs_sample.csv`. `VIETJOBS_CSV_PATH` trong `.env`
   là đường dẫn tương đối dưới repo (ví dụ `data/VietJobs_sample.csv`); Compose
   chuyển thành `/opt/airflow/data/VietJobs_sample.csv`. `PIPELINE_DATABASE_URL`,
   `PIPELINE_DATA_ROOT`, `PIPELINE_EXPORT_ROOT` và `AI_PLATFORM_IMPORT_URL` trong
   Compose dùng địa chỉ container, thay cho giá trị dành cho CLI host.
3. Chạy:

```bash
docker compose build
docker compose up -d
docker compose ps -a
docker compose exec airflow python -m pip check
docker compose exec airflow alembic check
```

Mở [Airflow UI](http://localhost:8080), đăng nhập bằng tài khoản trong `.env`.
`postgres` và `airflow` phải `healthy`; `airflow-init` phải `Exited (0)`.
DAG **giữ paused khi khởi động** (`DAGS_ARE_PAUSED_AT_CREATION=True`) vì
`catchup=True` sẽ chạy bù từ 2026-09-30 nếu unpause. `dags test` vẫn chạy DAG
paused và không cần bật scheduler cho DAG này.

Chạy sample bằng Airflow thật, không POST sang LLM (lệnh này đọc CSV local và
kết nối PostgreSQL của Compose):

```bash
docker compose exec -e VIETJOBS_CSV_PATH=/opt/airflow/data/VietJobs_sample.csv -e DELIVERY_ENABLED=false airflow airflow dags test job_pipeline_vietjobs "$(date +%F)"
# Chạy lại đúng lệnh trên để kiểm tra Bronze bất biến và Gold không tăng trùng.
docker compose exec postgres psql -U postgres -d job_pipeline -c 'SELECT run_id, status, input_count, valid_count, invalid_count, duplicate_count, delivered_count FROM pipeline_runs;'
```

Tám task phải success: `acquire`, `bronze`, `validate_clean`, `silver`,
`deduplicate_gold`, `quality_check`, `export_payload`, `deliver`.
`DELIVERY_ENABLED` mặc định **false trong Compose**; vẫn export và đóng run
`succeeded`, `delivered_count=0`. Với chính sách company mặc định, các record
VietJobs thiếu company được ghi `blocked` trong delivery metadata.

Khi API LLM đang chạy trên host cổng 8000 và bạn muốn thử delivery thật,
chạy lệnh sau (**gọi API, có thể kích hoạt LLM trả phí**):

```bash
docker compose exec -e DELIVERY_ENABLED=true -e DELIVERY_PAYLOAD_PROFILE=extended -e DELIVERY_MISSING_COMPANY=send_null airflow airflow dags test job_pipeline_vietjobs "$(date +%F)"
```

Chỉ dùng `extended` khi API hỗ trợ contract đó. Compose trỏ tới
`http://host.docker.internal:8000/jobs/import`; không dùng `localhost` trong
container và không truy cập PostgreSQL/Qdrant của LLM. Để áp dụng cấu hình
`.env` cho scheduler, chạy lại `docker compose up -d`.

Dữ liệu nằm trong bind mount `./data`; logs nằm trong volume `airflow-logs`,
config Airflow tự sinh nằm cùng volume ở `logs/airflow.cfg`. Dừng và giữ metadata:

```bash
docker compose down
```

Xóa **metadata PostgreSQL và logs của Crawl** để khởi tạo lại:

```bash
docker compose down -v
```

`down -v` không xóa `./data`; Gold/Bronze vẫn còn. Không dùng lệnh này nếu cần
giữ checkpoint delivery. Linux: đặt `AIRFLOW_UID` bằng `id -u` và đảm bảo
`data/` ghi được bởi UID đó; macOS Docker Desktop dùng mặc định `50000`.

### Kết quả chạy thật M8.2 — 2026-10-08

`docker compose build` thành công, `python -m pip check` không có conflict;
Airflow 2.10.5, SQLAlchemy 1.4.54, DuckDB 1.1.3, Polars 1.44.2.
PostgreSQL và Airflow healthy, init exit 0; `alembic check` không có migration
thiếu. Hai database có owner riêng và hai role không có quyền CONNECT chéo.

| CSV / lần chạy | Input | Valid | Invalid | Filtered | Duplicate | Gold |
|---|---:|---:|---:|---:|---:|---:|
| `VietJobs_sample.csv`, lần 1 | 5 | 0 | 0 | 5 | 0 | 0 |
| `VietJobs_sample.csv`, lần 2 | 5 | 0 | 0 | 5 | 0 | 0 |
| Subset IT local, lần 1 | 5 | 5 | 0 | 0 | 0 | 5 |
| Subset IT local, lần 2 | 5 | 5 | 0 | 0 | 5 | 5 |

Cả bốn lần đều có **8/8 task success**, gồm acquire, bronze, validate_clean,
silver, deduplicate_gold, quality_check, export_payload và deliver. Sample
có sẵn không thuộc IT, nên để kiểm tra Gold có dữ liệu đã lấy thêm năm dòng IT
thật từ `data/VietJobs.csv` vào `data/VietJobs_it_sample.csv` (git-ignored),
không sửa các CSV gốc. Hai lần bổ sung dùng cùng ngày với logical time
`2026-10-08T01:00:00+00:00` và override `VIETJOBS_CSV_PATH` trong lệnh `dags test`.

Bronze giữ nguyên checksum và mtime sau rerun; Gold IT giữ năm hash duy nhất.
Hai logical run trong `pipeline_runs` đều `succeeded`, `delivered_count=0`;
rerun cùng logical date cập nhật cùng run ID. Năm record IT bị `blocked` bởi
chính sách company mặc định. DAG vẫn paused, không có scheduled catchup run.
API LLM cổng 8000 phản hồi HTTP 200 ở `/openapi.json`; bỏ qua bước import live
tùy chọn, tất cả DAG run dùng `DELIVERY_ENABLED=false`.

Checks hiện có: focused DAG/M8 tests **19 passed**; `uv run pytest` **93 passed**
(hai Airflow deprecation warnings); Ruff lint và format pass. Tests dùng
fixtures/HTTP giả; các lần `dags test` ở trên dùng Airflow và PostgreSQL thật,
CSV local, không gọi website hoặc LLM.

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
| `DELIVERY_ENABLED` | `true` (Python), `false` (Compose) | `false` để chỉ export trong DAG |
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

`VietJobsDatasetSource.iter_raw()` đọc mọi dòng nguồn. Bộ lọc `is_technology_job(row)` được áp dụng sau khi đọc để quyết định dòng nào đi tiếp. Nó giữ category `công_nghệ_thông_tin_kỹ_thuật_số` và một số tiêu đề nghề công nghệ cụ thể ở category khác; không tìm từ khóa chung trong description. Trên toàn bộ file hiện tại, quy tắc giữ 2.043 dòng. Đây là quy tắc xác định phạm vi, chưa phải nhãn đúng tuyệt đối. `source_record_hash` dùng toàn bộ dòng CSV với thứ tự khóa ổn định, nên cùng nội dung có cùng hash khi chạy lại; nó không phải ID bền vững của một tin đã cập nhật.

## Thử Fake Jobs thủ công

Đoạn sau **dùng mạng**, lấy tối đa hai công việc và lưu HTML và metadata trong `data/raw_html/fake_jobs/`:

```bash
uv run python - <<'PY'
from itertools import islice

from job_pipeline.sources.fake_jobs import FakeJobsCrawler

with FakeJobsCrawler() as source:
    for raw in islice(source.iter_raw(), 2):
        print(raw["source_url"], raw["title"])
PY
```

Không commit dataset, raw dumps, Parquet output, `.env` hoặc credentials. Các file dữ liệu dùng khi chạy pipeline được lưu ở `data/` trên máy local.

Các biến cấu hình chung và Compose:

| Biến | Giá trị / ý nghĩa |
|---|---|
| `VIETJOBS_CSV_PATH` | CSV local; Compose thêm prefix `/opt/airflow/` |
| `PIPELINE_SOURCE` | Dự phòng khi task không có `params.source`: `vietjobs`, `fake_jobs` hoặc `arbeitnow` |
| `ARBEITNOW_MAX_PAGES` | `5`, số nguyên ≥ 1; giới hạn trang Arbeitnow cho CLI và Airflow |
| `ARBEITNOW_SCHEDULE` | Lịch DAG `job_pipeline_arbeitnow`, mặc định `@daily` |
| `PIPELINE_DATA_ROOT` | `data` trên host, `/opt/airflow/data` trong Compose |
| `PIPELINE_DATABASE_URL` | URL metadata DE; host dùng cổng `5433`, Compose dùng `postgres:5432` |
| `AI_PLATFORM_IMPORT_URL` | Host: `http://localhost:8000/jobs/import`; Compose: `http://host.docker.internal:8000/jobs/import` |
| `AI_PLATFORM_API_KEY` | Trống khi chạy local; đặt bằng `ADMIN_API_KEY` của platform khi platform bật bảo vệ. Gửi trong header `X-API-Key`, không ghi vào log |
| `POSTGRES_PASSWORD` | Mật khẩu role quản trị PostgreSQL của Crawl |
| `PIPELINE_DB_PASSWORD` | Mật khẩu role `job_pipeline` (Compose tạo URL DE) |
| `AIRFLOW_DB_PASSWORD` | Mật khẩu role `airflow` |
| `AIRFLOW_ADMIN_USERNAME` | Username UI tạo khi init |
| `AIRFLOW_ADMIN_PASSWORD` | Mật khẩu UI tạo khi init |
| `AIRFLOW_ADMIN_EMAIL` | Email admin, mặc định `admin@example.invalid` |
| `AIRFLOW_FERNET_KEY` | Fernet key hợp lệ để mã hóa Airflow connections |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | Secret dùng chung cho webserver và scheduler |
| `AIRFLOW_UID` | UID container, mặc định `50000` |


## Arbeitnow API — incremental

Nguồn public [Arbeitnow](https://www.arbeitnow.com/api/job-board-api?page=1)
không cần key. Adapter chỉ gọi HTTPS trên `www.arbeitnow.com`, không theo
redirect, timeout 30 giây, nghỉ ít nhất 1 giây giữa request; retry tối đa 3 lần
cho lỗi mạng, timeout, 429 và 5xx, có backoff và tôn trọng `Retry-After`.
URL trong `links.next` cũng được kiểm tra allowlist. Giữ URL tin tuyển dụng
làm `source_url` để dẫn về nguồn (URL của job có thể thuộc Arbeitnow Pháp).

Bronze giữ mọi job nguyên văn, kể cả tiếng Đức/Pháp và ngoài IT. Schema yêu
cầu đủ các key Arbeitnow, cho phép key mới; thiếu key vào quarantine
`SCHEMA_DRIFT`. Mapper giải mã HTML tối đa 3 lần rồi lấy text theo block,
map slug thành source ID, Unix timestamp thành UTC và đánh dấu Remote trong
location. Không suy diễn salary, experience hay skills. Hash SHA-256 dùng
JSON sorted-key của toàn bộ raw record cùng source name, không chứa thời
điểm thu thập; Gold dedup theo `(arbeitnow, slug)`.

Sau schema check và mapping, chỉ Arbeitnow đi qua bộ lọc **ưu tiên chính xác**
(thà bỏ sót job IT còn hơn để lọt job không phải IT hoặc không phải tiếng Anh).
Job phải qua cả ba lớp, trượt một lớp là bị loại:

1. `language`: trong description đã bỏ HTML, tỷ lệ stopword tiếng Anh ≥ 0.10
   và ≥ 2 lần tỷ lệ stopword tiếng Đức hoặc tiếng Pháp (loại tin song ngữ).
2. `not_it`: tiêu đề chứa vai trò IT rõ ràng (developer, software engineer,
   data scientist…) hoặc danh từ vai trò (engineer, architect, lead…) đi kèm
   từ IT (software, data, AI, cloud, security…); loại sales, marketing,
   manufacturing, product manager, consultant, designer… Tiêu đề chung chung
   như "Solutions Engineer" bị loại.
3. `no_tech_terms`: description phải nhắc ít nhất 2 công nghệ cụ thể khác nhau
   (Python, SQL, AWS, Docker, React…).

`filtered_count` gồm cả ba lý do, log riêng từng lý do; record bị lọc không vào
quarantine. Log cảnh báo khi tỷ lệ giữ lại nằm ngoài 3–50% (input ≥ 50), dấu
hiệu nguồn đổi định dạng. Trên 6 trang thật (1.050 job): giữ 140 job sau dedup.
Bộ lọc VietJobs không đổi.

CLI và Airflow cùng đọc `max(posted_at)` từ Gold Arbeitnow, trừ 60 phút làm
watermark; chưa có Gold hoặc chưa có ngày đăng thì dùng `None`. Adapter yield
toàn bộ trang rồi mới dừng khi **mọi** job của trang cũ hơn watermark, hết
`links.next`, hoặc đạt giới hạn trang. Một job cũ ghim đầu trang không làm
dừng sớm. Gold vẫn dedup dữ liệu trong khoảng overlap. Log acquisition có
`pages`, `stop_reason` và watermark để phân biệt lý do dừng.

```bash
# Có gọi mạng, chỉ chạy đến Gold/quality, không export/deliver hay gọi LLM.
ARBEITNOW_MAX_PAGES=1 uv run python -m job_pipeline run --source arbeitnow --root /tmp/arbeitnow-check
```

Airflow: bật DAG `job_pipeline_arbeitnow` (hoặc `airflow dags trigger job_pipeline_arbeitnow`); `ARBEITNOW_MAX_PAGES=5`.
`acquire_task` lưu snapshot JSON bất biến theo hash dưới
`data/acquired/source=arbeitnow/ingestion_date=<date>/records-<hash>.json`;
các task Bronze → quality → export → deliver dùng luồng chung. Trong DAG,
`DELIVERY_ENABLED=false` vẫn export nhưng không POST như các nguồn khác.

Giới hạn incremental: watermark lấy từ job đã qua lọc trong Gold, nên có
thể đọc lại nhiều trang nếu không có job IT tiếng Anh mới. Với giới hạn một
trang, lần chạy lại thường dừng vì `max_pages`, không phải watermark:
trang vẫn chứa job mới hơn `max(posted_at) - 60 phút`. Các trang cũ hơn giới
hạn chưa được backfill; đây là polling có giới hạn, không phải full backfill.
