# Milestone 8 — Delivery-ready pipeline

> File này là yêu cầu được user cho phép cho milestone hiện tại. Với milestone
> này, nó **override mục 7 (Gold contract) và mục 11 (Delivery)** của
> `docs/PROJECT_SPEC.md`. Các mục khác của spec và `AGENTS.md` vẫn áp dụng.
> **Không sửa repo `LLM/` (AI Career Platform)** và không sửa
> `docs/PROJECT_SPEC.md`.

## 1. Bối cảnh

M1–M6 đã chạy: VietJobs CSV + Fake Jobs → Bronze → Silver → Gold →
`DeliveryMapper` → `POST /jobs/import`. Gold thật hiện có 2.040 job IT
VietJobs, quarantine 0, 71 test pass. Shape payload hiện khớp
`RawJobRecord` bên AI Platform (JSON array, multipart `file=jobs.json`, < 1 MB).

Vấn đề cần giải quyết trong milestone này (chỉ phía Crawl):

1. VietJobs không có company → `DeliveryMapper` block 2.040/2.040 record, run
   luôn `failed`, DAG fail mỗi ngày.
2. Gold gộp description + requirements + experience + skills + qualifications
   vào một cột `raw_description` và bỏ mất salary, benefits, category,
   working_hours, languages, soft_skills, country. Bảng bên AI Platform mới là
   bản demo, sau này sẽ chỉnh schema/embedding, nên Gold phải giữ các field gốc
   có cấu trúc.
3. Batch chỉ giới hạn theo byte (~500 job/batch), timeout 30 s cố định, trong
   khi AI Platform gọi LLM tuần tự cho từng record trong cùng request → chắc
   chắn timeout → retry → trích xuất lại.
4. Response có `failed > 0` → cả batch bị đánh `failed` và retry toàn bộ;
   record lỗi retry mãi; không có checkpoint cấp record.
5. Không có cách xem trước payload (dry-run/export) và không có CLI chạy tay.

Nguyên tắc giữ nguyên: không bịa giá trị (`null`), transform deterministic và
idempotent, hash không chứa thời điểm thu thập, test chỉ dùng fixture/HTTP giả.

## 2. Phase 0 — Baseline

- Chạy `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`
  trước khi sửa và ghi lại kết quả.
- Không commit `data/`, Parquet, file export, `.env`.

## 3. Phase 1 — Gold contract v2

### 3.1 `CanonicalJob` (`src/job_pipeline/contracts.py`)

- `schema_version` mặc định `2`.
- Thêm các field `str | None = None`, lấy **nguyên văn** từ nguồn (chỉ qua
  `clean_text` ở Silver như các field text khác):

| Field Gold | Cột VietJobs |
|---|---|
| `raw_requirements` | `requirements_text` |
| `raw_experience` | `experience_required` |
| `raw_technical_skills` | `technical_skills` |
| `raw_soft_skills` | `soft_skills` |
| `raw_qualifications` | `qualifications` |
| `raw_languages` | `languages_required` |
| `raw_benefits` | `benefits` |
| `raw_salary` | `salary` |
| `raw_working_hours` | `working_hours` |
| `raw_category` | `category` |
| `raw_country` | `country` |

- Ô rỗng/chỉ khoảng trắng → `None`. Không parse lương, không đổi đơn vị,
  không tách list skill, không suy diễn required/preferred.
- Fake Jobs: tất cả field mới là `None`.

### 3.2 VietJobs mapping (`src/job_pipeline/sources/vietjobs.py`)

- `raw_description` = **chỉ** cột `description`.
- Các cột ở bảng trên map vào field mới.
- **Không đổi** cách tính `source_record_hash` (hash cả dòng, sorted keys). Cùng
  một dòng CSV phải cho cùng hash như v1 → `record_key = source_name:hash` cũ
  vẫn hợp lệ cho checkpoint.

### 3.3 Mô tả gửi đi

- Chuyển logic `compose_vietjobs_description` thành hàm chung
  `compose_import_description(row: Mapping) -> str` trong
  `src/job_pipeline/delivery.py`, đọc từ các cột Gold theo thứ tự nhãn cũ:
  `Description` (`raw_description`), `Requirements` (`raw_requirements`),
  `Experience` (`raw_experience`), `Technical skills` (`raw_technical_skills`),
  `Qualifications` (`raw_qualifications`). Giữ quy tắc bỏ section trùng
  (so sánh casefold + gộp khoảng trắng) và quy tắc "chỉ có Description thì trả
  text trần không nhãn".
- Với Fake Jobs (chỉ có description) output = `raw_description`.
- Test: với fixture VietJobs, output mới phải **giống hệt** output
  `compose_vietjobs_description` của v1 (sau cùng bước clean Silver).
- Xoá hoặc để `compose_vietjobs_description` gọi hàm mới; không giữ hai bản
  logic song song.

### 3.4 Cập nhật đồng bộ

- `validate.py::ValidatedJob`: thêm field mới, `schema_version: Literal[2]`,
  vẫn `strict=True, extra="forbid"`.
- `pipeline.py::CANONICAL_SCHEMA` và `_empty_jobs`: thêm cột mới kiểu
  `pl.String`.
- `clean.py::TEXT_FIELDS`: thêm field mới (giữ nguyên hành vi giữ xuống dòng).
- `sql/quality_checks.sql`: thêm null count cho cột mới.
- Khi đọc Gold/Silver thiếu cột v2 (dữ liệu v1 cũ trên máy) → raise lỗi rõ
  ràng kiểu `"Gold schema v1 detected at <path>; rerun the pipeline"`. Không
  migrate ngầm.

**Acceptance:** chạy pipeline trên `tests/fixtures/vietjobs_small.csv` tạo
Gold có đủ cột v2, `schema_version = 2`, hash trùng với v1.

## 4. Phase 2 — Quality report

`src/job_pipeline/quality.py::quality_report` bổ sung (giữ các key cũ):

- `schema_versions`: `{version: count}`.
- `nulls`: thêm null count cho mọi field mới.
- `missing_company`: số row `raw_company IS NULL`.
- `near_duplicates`: số row thừa khi group theo
  `(raw_title, raw_location, raw_employment_type, raw_description)` và
  `count > 1` (dữ liệu thật hiện ~25). **Chỉ báo cáo, không xoá.**
- `description_bytes`: `{min, p50, max}` của độ dài UTF-8 `raw_description`.

`quality_check_task` giữ gate cũ (missing title/description = 0,
duplicates = 0); metric mới chỉ log.

## 5. Phase 3 — Mapper profile, company policy và export (dry-run)

### 5.1 Cấu hình (thêm vào `.env.example`, đọc qua một dataclass config nhỏ)

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `DELIVERY_PAYLOAD_PROFILE` | `ai_platform_v1` | `ai_platform_v1` \| `extended` |
| `DELIVERY_MISSING_COMPANY` | `block` | `block` \| `send_null` |
| `DELIVERY_MAX_BYTES` | `1000000` | file JSON phải **nhỏ hơn** giá trị này |
| `DELIVERY_MAX_RECORDS` | `20` | số record tối đa mỗi batch |
| `DELIVERY_TIMEOUT_SECONDS` | `300` | timeout mỗi POST |
| `DELIVERY_MAX_ATTEMPTS` | `3` | số lần thử cho lỗi retryable |
| `DELIVERY_ENABLED` | `true` | `false` → DAG dừng sau export |
| `PIPELINE_EXPORT_ROOT` | `data/export` | thư mục ghi batch |

Giá trị không hợp lệ → `ValueError` rõ ràng khi khởi tạo config.

### 5.2 `DeliveryMapper`

- `ai_platform_v1`: đúng 6 key `title, company, location, employment_type,
  description, source_url`; `description = compose_import_description(row)`.
- `extended`: 6 key trên + `source_name, source_record_hash, posted_at`
  (ISO 8601 hoặc `null`), `requirements, experience, technical_skills,
  soft_skills, qualifications, languages, benefits, salary, working_hours,
  category, country` (lấy từ cột `raw_*` tương ứng). Không đổi mặc định sang
  profile này.
- Company:
  - `block`: company rỗng → record `blocked` (hành vi hiện tại).
  - `send_null`: gửi `"company": null`. Không bao giờ tạo placeholder.
- Key luôn có mặt (giá trị `null` nếu thiếu); JSON serialize ổn định
  (`ensure_ascii=False, sort_keys=True, separators=(",", ":")`).

### 5.3 Batching

- `serialize_batches(..., max_bytes, max_records)`: đóng batch khi thêm record
  kế tiếp làm file `>= max_bytes` **hoặc** vượt `max_records`.
- Một record đơn lẻ không vừa `max_bytes` → `OversizeRecordError` (giữ như cũ).

### 5.4 Export

Hàm mới `export_batches(gold_path, *, run_id, sessions, out_dir, config,
retry_rejected=False, retry_blocked=False) -> ExportManifest`:

1. Đọc Gold, loại record đã `accepted` (và `rejected`/`blocked` trừ khi bật
   cờ retry tương ứng) theo checkpoint ở Phase 4.
2. Map theo profile/policy; record bị block ghi checkpoint `blocked`.
3. Chia batch, ghi:

```text
<PIPELINE_EXPORT_ROOT>/source=<source>/run_id=<run_id>/batch-0001.json
<PIPELINE_EXPORT_ROOT>/source=<source>/run_id=<run_id>/manifest.json
```

`manifest.json` gồm: `run_id, source_name, gold_path, profile,
missing_company_policy, max_bytes, max_records, created_at, blocked_count`
và danh sách `batches[{file, payload_hash, record_count, bytes, record_keys}]`.

4. Idempotent: cùng Gold + cùng checkpoint + cùng config → cùng file và cùng
   `payload_hash` (ghi file qua tmp + rename; nội dung giống thì không đổi).
5. Không gọi HTTP.

## 6. Phase 4 — Delivery hardening

### 6.1 Gửi từ manifest

`deliver_gold` đổi thành gửi các batch trong manifest đã export (đọc bytes từ
file, kiểm tra lại `payload_hash` trước khi gửi; lệch → lỗi, không gửi). Giữ
multipart field `file`, filename `jobs.json`, content-type `application/json`.

### 6.2 Xử lý response

| Tình huống | Hành vi |
|---|---|
| 2xx, body hợp lệ (`processed == record_count`) | batch `succeeded`; record có index trong `errors[].record` (1-based, theo thứ tự trong file) → `rejected` + `error`; record còn lại → `accepted` (inserted hoặc duplicate). **Không retry** vì có record bị reject. |
| 2xx nhưng body sai/thiếu field/`processed` lệch/index ngoài phạm vi | batch `failed`, không retry |
| timeout, lỗi mạng, 429, 500, 502, 503, 504 | retry có backoff tới `DELIVERY_MAX_ATTEMPTS`; hết lượt → `failed` |
| 413 | `failed`, không retry, `last_error` nêu vượt giới hạn upload |
| 4xx khác | `failed`, không retry |

Giữ cơ chế `sending` + cửa sổ 10 phút hiện có để tránh gửi đồng thời.

### 6.3 Checkpoint cấp record — migration `migrations/versions/0002_delivery_records.py`

- Bảng `delivery_records`:
  - `record_key` String PK (`source_name:source_record_hash`)
  - `source_name` String(50) NOT NULL
  - `last_batch_id` String(36) FK → `delivery_batches.batch_id`, nullable
  - `status` String(20) NOT NULL, CHECK IN (`accepted`, `rejected`, `blocked`)
  - `error` Text nullable
  - `updated_at` DateTime(timezone=True) NOT NULL
- `pipeline_runs`: thêm `blocked_count`, `rejected_count` Integer NOT NULL
  default 0.
- Cập nhật ORM trong `metadata.py`; `alembic upgrade head`, `downgrade -1` và
  `alembic check` phải sạch trên SQLite.
- `checkpoints.py`:
  - `record_statuses(session) -> dict[str, str]`.
  - Record key trong batch `succeeded` cũ (trước migration, chưa có dòng
    `delivery_records`) vẫn tính là `accepted`.
  - Hàm upsert trạng thái record.
- `rejected`/`blocked` không tự gửi lại; chỉ gửi lại khi
  `retry_rejected`/`retry_blocked` = True.

### 6.4 Trạng thái run

- `DeliveryReport` thêm `rejected_records`.
- Run `succeeded` khi không có batch `failed`. Blocked/rejected chỉ ghi count
  vào `pipeline_runs` + log warning, **không** làm task/DAG fail.
- Còn batch `failed` → run `failed`, task raise để Airflow retry (như cũ).
- Áp dụng giống nhau cho `service.py` và `workflow.py`.

## 7. Phase 5 — Orchestration và CLI

### 7.1 DAG

```text
acquire → bronze → validate_clean → silver → deduplicate_gold
→ quality_check → export_payload → deliver
```

- Thêm `export_payload_task` trong `workflow.py` (trả đường dẫn manifest qua
  XCom); `deliver_task` đọc manifest đó.
- `DELIVERY_ENABLED=false` → `deliver_task` bỏ qua và kết thúc run
  `succeeded` với `delivered_count=0`.
- Business logic ở module Python; DAG chỉ nối task.

### 7.2 CLI `python -m job_pipeline` (argparse, không thêm dependency)

Tạo `src/job_pipeline/__main__.py` (và `cli.py` nếu cần):

```bash
uv run python -m job_pipeline run --source vietjobs --csv data/VietJobs.csv [--root data]
uv run python -m job_pipeline export --source vietjobs [--root data] [--retry-rejected] [--retry-blocked]
uv run python -m job_pipeline deliver --source vietjobs --endpoint http://localhost:8000/jobs/import [--manifest PATH]
```

- `run`: Bronze→Gold + in `quality_report` dạng JSON. Không cần DB.
- `export`/`deliver`: cần `PIPELINE_DATABASE_URL`; in summary JSON (đường dẫn
  manifest, số batch, accepted/rejected/blocked/failed).
- `deliver` không truyền `--manifest` → dùng manifest mới nhất của source.
- `fake_jobs` qua CLI được phép dùng crawler thật (mạng) chỉ khi user chạy tay;
  test CLI chỉ dùng VietJobs fixture.
- Exit code ≠ 0 khi có batch `failed` hoặc lỗi cấu hình.

## 8. Phase 6 — Tài liệu

- Cập nhật `README.md`: Gold v2, bảng profile payload, company policy, biến
  môi trường, lệnh CLI, cách xem `delivery_records`.
- Ghi rõ giới hạn đã biết: AI Platform hiện yêu cầu company và có taxonomy
  skill rất nhỏ → nhiều record sẽ bị `rejected` cho tới khi AI Platform được
  chỉnh. Đây là hành vi mong đợi, không phải lỗi Crawl.
- Không sửa `docs/PROJECT_SPEC.md`.

## 9. Ngoài phạm vi

Mọi thay đổi trong `LLM/`; Docker Compose; AWS/Kafka/Snowflake; nguồn crawl
mới; parse lương/skill; dedup gần đúng (near-duplicate chỉ báo cáo).

## 10. Tests bắt buộc

Dùng fixture và `httpx.MockTransport`, không gọi mạng hoặc LLM.

1. VietJobs v2 mapping: từng cột gốc vào đúng field; ô rỗng → `None`; hash
   bằng hash v1 cho cùng dòng.
2. `compose_import_description` khớp output v1 trên fixture; Fake Jobs trả
   description trần.
3. Validation v2: `schema_version=1` hoặc field thừa → quarantine với
   `error_code`.
4. Đọc Gold v1 (thiếu cột) → lỗi "rerun the pipeline".
5. Quality report: key mới có mặt; near-duplicate được đếm và vẫn còn trong
   Gold.
6. Mapper: `ai_platform_v1`/`extended` × `block`/`send_null` (đúng tập key,
   `company: null`, block ghi checkpoint).
7. Batching: giới hạn `max_records`, giới hạn `max_bytes` (strict <), record
   đơn quá lớn → `OversizeRecordError`.
8. Export idempotent: chạy 2 lần → cùng file, cùng `payload_hash`; không gọi
   HTTP.
9. Deliver kiểm tra `payload_hash` file bị sửa → không gửi.
10. Partial response `{"processed":3,"inserted":2,"skipped_duplicates":0,
    "failed":1,"errors":[{"record":2,"error":"x"}]}` → 2 accepted, 1 rejected,
    batch `succeeded`, 1 lần POST; rerun không gửi lại gì.
11. `--retry-rejected` gửi lại đúng record rejected.
12. 413 → 1 lần POST, `failed`; 503 rồi 200 → `succeeded`; timeout hết lượt →
    `failed`, rerun gửi lại.
13. Batch `succeeded` trước migration vẫn tính là accepted.
14. Migration 0002 upgrade/downgrade trên SQLite; `alembic check` sạch.
15. Run status: chỉ blocked/rejected → run `succeeded` với count đúng; có batch
    failed → run `failed` và task raise.
16. DAG import + thứ tự task mới; `DELIVERY_ENABLED=false` bỏ qua deliver.
17. CLI `run`/`export`/`deliver` trên fixture với `tmp_path` và SQLite; exit
    code đúng.
18. E2E: CSV fixture → Gold v2 → export → fake `/jobs/import` → rerun → không
    gửi trùng.

## 11. Definition of done

- `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .` pass.
- `uv run alembic upgrade head && uv run alembic check` sạch (SQLite tạm).
- Chạy local trên `data/VietJobs.csv`: Gold ~2.040 dòng, `schema_version=2`,
  cột mới có dữ liệu; `export` với `DELIVERY_MISSING_COMPANY=send_null` tạo
  batch ≤ 20 record, < 1 MB, đúng 6 key, `company: null`.
- Báo cáo cuối: file đã sửa, lệnh đã chạy, kết quả thật, giới hạn còn lại
  (theo mục "Before completion" trong `AGENTS.md`).
