# Current milestone — Milestone 8.1: Dọn luồng acquisition (một đường duy nhất)

> **Trạng thái: HOÀN THÀNH.** Không implement lại; chờ milestone mới ghi đè file này.

> File này là yêu cầu được user cho phép cho milestone hiện tại và sẽ được ghi
> đè ở milestone sau. Milestone 8 (Delivery-ready) đã hoàn thành; xem git
> history. Milestone 9 trong `../LLM/docs/CURRENT_MILESTONE.md` thuộc repo LLM,
> **không** làm ở đây. Milestone 9 Kafka/Snowflake trong `docs/PROJECT_SPEC.md`
> **không** được yêu cầu.
>
> Đây là **refactor thuần**: không đổi hành vi, không đổi Gold output, không đổi
> hash, không đổi delivery. Không sửa repo `../LLM` và `docs/PROJECT_SPEC.md`.

## 1. Vấn đề

Code đang có **hai đường** đưa dữ liệu nguồn thành `CanonicalJob`, làm người đọc
không biết luồng chính là gì:

| | Luồng chính (CLI, DAG, `service.py`) | Đường cũ từ Milestone 1–2 |
|---|---|---|
| Lấy dữ liệu | `VietJobsDatasetSource.iter_rows()`, `FakeJobsCrawler.iter_records()` | `iter_jobs()` của cả hai nguồn |
| Map sang `CanonicalJob` | **Sau Bronze**, trong `stages.validate_clean_bronze` qua `pipeline._map_job` | Map ngay lúc lấy, **bỏ qua Bronze/quarantine** |
| Ghi thêm | `data/raw_html/` (Fake Jobs) | `data/parsed/fake_jobs/` qua `persist_parsed_record` |
| Ai dùng | Production | Chỉ tests + ví dụ README |

Các mảnh thừa khác:

- `sources/base.py::SourceAdapter` mô tả `iter_jobs`, không phải interface thật
  mà pipeline dùng.
- `sources/vietjobs.py::compose_vietjobs_description` chỉ còn là wrapper cho
  test; logic thật ở `delivery.py::compose_import_description`.
- `pipeline.py::_map_job` dựng lại `FakeJobRecord` từ dict rồi mới gọi
  `map_fake_job`, trong khi VietJobs map thẳng từ dict → hai nguồn không đối xứng.
- Tên hàm lấy dữ liệu khác nhau giữa hai nguồn (`iter_rows` vs `iter_records`).

## 2. Mục tiêu sau khi dọn

Mỗi nguồn chỉ có **đúng hai việc**, cùng tên, cùng kiểu:

```text
Source.iter_raw()  → dict thô, giữ nguyên field nguồn   (acquire: đọc file / HTTP)
map_<source>_row(raw: Mapping, *, collected_at) → CanonicalJob   (hàm thuần, không I/O)
```

Luồng duy nhất:

```text
iter_raw() → snapshot_bronze() → validate_clean_bronze() [map_<source>_row ở đây]
          → publish_silver() → deduplicate_gold() → quality → export → deliver
```

## 3. Việc cần làm

### 3.1 Interface nguồn (`src/job_pipeline/sources/base.py`)

Thay `SourceAdapter` bằng protocol phản ánh đúng thực tế:

```python
class RawSource(Protocol):
    source_name: str

    def iter_raw(self) -> Iterator[dict[str, object]]: ...
```

Cập nhật docstring module (bỏ chữ "milestone 1").

### 3.2 VietJobs (`src/job_pipeline/sources/vietjobs.py`)

- `VietJobsDatasetSource`: thêm `source_name = "vietjobs"`; đổi `iter_rows()`
  thành `iter_raw()` (giữ nguyên hành vi: kiểm cột bắt buộc, yield dict).
- **Xoá** `iter_jobs()` và `compose_vietjobs_description()`.
- Giữ `map_vietjobs_row(row, *, collected_at)` và `SOURCE_FIELDS` như hiện tại
  (không đổi hash).

### 3.3 Fake Jobs (`src/job_pipeline/sources/fake_jobs.py`)

- `FakeJobsCrawler`: thêm `source_name = "fake_jobs"`; thêm `iter_raw()` yield
  dict có đúng các key `FAKE_JOBS_COLUMNS` (`title, company, location,
  description, source_url, posted_at`), `posted_at` là chuỗi ISO 8601 hoặc
  `None` — tức đúng dạng mà `run_fake_jobs` đang ghi vào Bronze qua `asdict`.
  Giữ việc lưu raw HTML + metadata trong `fetch_page` (đó là audit hợp lệ của
  bước acquire).
- `iter_records()` có thể giữ làm hàm nội bộ trả `FakeJobRecord` cho
  `iter_raw()` dùng; không gọi từ ngoài module.
- **Xoá** `iter_jobs()`, `persist_parsed_record()`, tham số
  `parsed_records_dir` và mặc định `data/parsed/fake_jobs`.
- Thay `map_fake_job(record: FakeJobRecord, ...)` bằng
  `map_fake_jobs_row(raw: Mapping[str, object], *, collected_at)` nhận dict
  (parse `posted_at` từ chuỗi ISO bên trong). Hash phải **giống hệt** hash cũ cho
  cùng record.

### 3.4 Pipeline (`src/job_pipeline/pipeline.py`, `stages.py`)

- Thay `_map_job` bằng bảng tra cứu rõ ràng:

```python
MAPPERS = {"vietjobs": map_vietjobs_row, "fake_jobs": map_fake_jobs_row}
```

  `validate_clean_bronze` gọi `MAPPERS[source](raw, collected_at=...)`; source
  lạ → `ValueError` như hiện tại.
- `read_vietjobs_rows` dùng `iter_raw()`.
- `run_fake_jobs(records, ...)` nhận iterable dict từ `iter_raw()` (bỏ `asdict`).
  Cập nhật mọi chỗ gọi: `workflow.py`, `__main__.py`, `service.py`.
- Thêm docstring ngắn ở đầu `pipeline.py` vẽ đúng luồng ở mục 2.

### 3.5 Tests

Chuyển test đang dùng hàm bị xoá sang hàm còn lại, **giữ nguyên ý nghĩa kiểm
tra**:

| Test hiện tại | Đổi thành |
|---|---|
| `tests/test_sources_unified.py` (dùng `SourceAdapter`, `iter_jobs`) | Kiểm cả hai nguồn thoả `RawSource`; `iter_raw()` + `map_<source>_row` cho cùng `CanonicalJob` như trước |
| `tests/test_vietjobs.py` (`iter_jobs`, `compose_vietjobs_description`) | `iter_raw()` + `map_vietjobs_row`; test ghép mô tả gọi `compose_import_description` với key Gold (`raw_description`, `raw_requirements`, …) và **giữ nguyên chuỗi kỳ vọng** |
| `tests/test_fake_jobs.py` (`iter_jobs`, `parsed_records_dir`, đếm file `parsed/*.json`) | `iter_raw()` + `map_fake_jobs_row`; giữ kiểm tra: số request, số file HTML, hash ổn định giữa hai lần crawl. Bỏ phần kiểm `parsed/` |
| `tests/test_milestone8.py::test_vietjobs_v2_fields_hash_and_description` (`compose_vietjobs_description`) | So `compose_import_description` với **chuỗi kỳ vọng cố định** viết thẳng trong test |

Thêm 1 test hồi quy **trước khi refactor**: chạy `run_pipeline` trên fixture
VietJobs và fixture Fake Jobs, lưu snapshot Gold (bỏ cột `collected_at`,
`bronze_path`) vào `tests/fixtures/` hoặc giá trị hash tổng trong test; sau
refactor test này phải pass y nguyên.

### 3.6 Tài liệu

- `README.md`:
  - Thêm mục **"Luồng chính"** với sơ đồ ở mục 2 và bảng "file nào làm bước
    nào" (`sources/*` → `stages.py` → `delivery.py`).
  - Sửa đoạn "Thử Fake Jobs thủ công": dùng `iter_raw()` (hoặc
    `python -m job_pipeline run --source fake_jobs`), bỏ nhắc tới bản ghi đã
    parse trong `data/parsed/`.
  - Sửa câu nhắc `iter_rows()` / `iter_records()` thành `iter_raw()`.
- Không sửa `docs/PROJECT_SPEC.md`.

## 4. Không làm

- Không đổi `CanonicalJob`, `validate.py`, logic lọc IT, dedup, quality,
  export, delivery, migrations, DAG task graph.
- Không thêm nguồn mới, không thêm dependency.
- Không xoá dữ liệu thật trong `data/` của user (thư mục `data/parsed/` cũ cứ để
  đó; README chỉ cần ghi là không còn được dùng).

## 5. Definition of done

- `grep -rn "iter_jobs\|persist_parsed_record\|parsed_records_dir\|compose_vietjobs_description\|SourceAdapter\|_map_job\|iter_rows\|map_fake_job\b" src dags tests README.md`
  không còn kết quả (trừ changelog/git).
- Test hồi quy Gold snapshot pass; toàn bộ `uv run pytest`,
  `uv run ruff check .`, `uv run ruff format --check .` pass.
- Chạy thật: `uv run python -m job_pipeline run --source vietjobs --csv data/VietJobs.csv --root <thư mục tạm>`
  cho cùng `count`, `near_duplicates` và tập `source_record_hash` như trước
  refactor (2.040 dòng).
- Báo cáo cuối theo mục "Before completion" trong `AGENTS.md`: file sửa/xoá,
  lệnh đã chạy, kết quả thật.
