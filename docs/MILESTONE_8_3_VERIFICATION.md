# Milestone 8.3 — báo cáo kiểm tra

Ngày kiểm tra: 2026-10-09 (Asia/Ho_Chi_Minh).

## Thay đổi

- `sources/arbeitnow.py`: HTTP acquisition có allowlist, rate limit, retry,
  kiểm schema response, incremental stop; mapper/hash/HTML và lọc tiếng Anh.
- `sources/technology.py`: bộ lọc tiêu đề tiếng Anh dùng lại mẫu hiện có,
  bổ sung ML/AI research, platform/security/QA; giữ nguyên VietJobs.
- `contracts.py`, `validate.py`, `pipeline.py`, `stages.py`: source mới,
  registry, schema cho phép key thừa, filter reasons, watermark từ Gold.
  Canonical schema vẫn v2 vì không thay cột hoặc payload contract.
- `__main__.py`, `workflow.py`, `config.py`: CLI/Airflow dùng chung config và
  watermark; acquired snapshot và source asset trỏ đúng Arbeitnow.
- `docker-compose.yml`, `.env.example`: truyền biến `ARBEITNOW_MAX_PAGES`.
- `tests/test_arbeitnow.py`, `tests/test_dag.py`: tests fixture và workflow.
- README, trạng thái milestone và Serena `core`, `conventions`,
  `suggested_commands`, `task_completion` đã cập nhật. `AGENTS.md` đã có
  Arbeitnow trong thay đổi sẵn của user nên giữ nguyên; hai fixture có sẵn
  được dùng nguyên. Không đổi PROJECT_SPEC, dependency, lockfile, DB schema,
  migration hay repository `../LLM`.

## Kiểm tra deterministic

- `.venv/bin/pytest -q tests/test_arbeitnow.py tests/test_dag.py`:
  **87 passed**; gồm schema drift, bad HTML/values, 404/429/503, timeout/network,
  retry exhaustion, redirects/host lạ, mọi job cũ vs một job cũ ghim đầu,
  ngưỡng tiếng Anh, IT, CLI/workflow, empty/all-filtered, rerun/dedup.
- Fixture hai trang: Bronze **15**, `language` **6**, `not_it` **1**,
  quarantine **0**, Gold **8**; rerun **8 duplicates**, Gold không đổi
  (số sau khi mở rộng bộ lọc IT, xem mục cuối).
- Workflow acquire → quality chạy hai lần bằng MockTransport và metadata
  SQLite có migrations; input=15, valid=6, invalid=0, duplicate=0 rồi 6.
- Toàn bộ suite, Ruff và kiểm tra Compose: xem kết quả cuối bên dưới.
- Test cũ VietJobs/Fake Jobs, kể cả acquisition regression, đều chạy cùng suite.
  HTTP trong tests là MockTransport; không gọi website hay API trả phí.

## Kiểm tra API thật, delivery tắt

Hai lần chạy cùng lệnh (mỗi lần chỉ một trang):

```bash
ARBEITNOW_MAX_PAGES=1 DELIVERY_ENABLED=false .venv/bin/python -m job_pipeline run --source arbeitnow --root /tmp/crawl-m83-live.nnTACl
```

Sandbox ban đầu chặn DNS; lần gọi lại qua quyền mạng được duyệt thành công.
Không chạy `export`, `deliver`, không gọi AI Platform hay DeepSeek.
Raw/Parquet/log nằm ngoài repo trong thư mục tạm nêu trên.

| Chỉ số | Lần 1 | Lần 2 |
|---|---:|---:|
| Trang HTTP thành công | 1 | 1 |
| Bronze jobs | 325 | 325 |
| Lọc `language` | 122 | 122 |
| Lọc `not_it` | 157 | 157 |
| Quarantine | 0 | 0 |
| Gold jobs | 46 | 46 |
| Duplicate trong Gold | 0 | 0 |
| Lý do dừng | max_pages | max_pages |

Checksum SHA-256 của Gold giống hệt sau hai lần; Bronze dùng lại snapshot
bất biến. Lần hai đã nạp watermark **2026-10-09 12:09:36 UTC**.
Điều kiện dừng do watermark yêu cầu **mọi** job trong trang cũ hơn ngưỡng;
trang đầu vẫn có job mới hơn ngưỡng nên dừng vì giới hạn 1 trang. Không báo
nhầm đây là dừng do watermark. Tests fixture xác nhận riêng nhánh watermark.

## 20 tiêu đề Gold đầu tiên

1. Junior Software Engineer, Data
2. Data Scientist
3. Staff Data Scientist
4. Staff Frontend Engineer / Frontend Architect (m/f/d)
5. Principal Software Engineer (Java) - Security - Elasticsearch
6. Senior Software Engineer
7. Senior Full Stack Engineer (Vue3, TypeScript)
8. AI Engineer
9. Graduate AI Engineer
10. Senior AI Engineer
11. Member of Technical Staff - Software Engineer
12. Senior Software Engineer
13. Data Architect
14. Forward Deployed AI Engineer
15. Senior Software Engineer - Applied AI
16. Machine Learning Engineer
17. Staff Software Engineer, Console GPU Inference
18. Senior Backend Engineer, Core DevOps: Container Registry
19. Senior Backend Engineer, Core DevOps
20. Cybersecurity Threat Researcher (Position located in Cheltenham, United Kingdom)

## 10 tiêu đề bị lọc not_it có engineer/developer/data

Có tổng cộng **37** tiêu đề khớp chuỗi này trong nhóm not_it. Dưới đây là 10
đầu tiên theo thứ tự nguồn; hai Technical Solutions Engineer là hai tin riêng.

1. Senior Sales Engineer, DACH Channel
2. Engineering Manager (all genders)
3. Scientist, Model Validation and Experimental Data Analysis
4. Data Analyst (m/f/d)
5. Senior Odoo Engineer ERP & E-Commerce (all genders)
6. Sales Engineer (m/f/d)
7. Technical Solutions Engineer
8. Technical Solutions Engineer
9. Senior Solutions Architect (EDW Enterprise Data Warehouse Migrations)
10. Forward Deployed Engineer

## Giới hạn

- Bộ lọc title có thể bỏ sót Data Analyst, Odoo Engineer, Solutions Engineer,
  Forward Deployed Engineer và quản lý kỹ thuật. Giữ nguyên phạm vi milestone;
  danh sách trên để user quyết định mở rộng trong yêu cầu sau.
- Ngôn ngữ là heuristic stopword, không phải mô hình nhận diện ngôn ngữ.
- Watermark dựa trên Gold sau lọc; có thể đọc lại nhiều trang và không phải
  checkpoint đầy đủ của nguồn. Giới hạn trang không đảm bảo full backfill.
- ~~`Remote, Remote`~~: đã sửa trong review (không ghép khi location đã chứa remote).
- Không có thay đổi DB/migration. Chạy các migrations hiện có trong tests
  SQLite; không kiểm lại migrations trên PostgreSQL thật cho milestone này.
- Đã kiểm tra cấu hình Compose và DAG/workflow bằng tests; không rebuild hay
  khởi động lại Docker/Airflow thật (không đổi image/dependencies/migrations).
- Dữ liệu API thay đổi theo giờ; số live là ảnh chụp tại lần kiểm tra này.

## Kết quả kiểm tra cuối

| Lệnh | Kết quả |
|---|---|
| `.venv/bin/pytest -q` | **176 passed**, 2 warning deprecation từ Airflow 2 về đơn vị timer metrics |
| `.venv/bin/ruff check .` | **All checks passed** |
| `.venv/bin/ruff format --check .` | **43 files already formatted** |
| `docker compose config --quiet` | Exit 0 |
| `git diff --check` | Exit 0 |
| `shasum -a 256 -c /tmp/crawl-m83-live.nnTACl/gold-before.sha256` | Gold **OK** sau lần live thứ hai |

Dùng executables trong `.venv` hiện có vì `uv run` bị sandbox chặn truy cập
cache tại `~/.cache/uv`; không cài hoặc đổi dependency để chạy kiểm tra.

## Sửa sau review (Claude)

- Bộ lọc IT tiếng Anh (`is_technology_title`) quá hẹp: loại nhầm Computer Vision
  Engineer, AI Solutions Engineer, Data Analyst, Solutions Architect, Forward
  Deployed Engineer… Đổi sang: giữ tiêu đề có vai trò kỹ thuật (engineer,
  developer, architect, scientist, data/systems/security analyst, devops, sre,
  CTO…), loại sales/marketing, manufacturing, electrical, mechanical, optical,
  hardware, architectural, payment, people, recruiting. Bộ lọc VietJobs không đổi.
- Không ghép `, Remote` khi location đã chứa "remote".
- Chạy lại offline trên trang đã lưu (325 job, không gọi API): `language` 122,
  `not_it` 121, quarantine 0, **Gold 82**, không còn `Remote, Remote`.
  Còn giữ vài ca mơ hồ (ví dụ "Founder Associate to the CTO",
  "Graduate Applications Engineer - Rental and Rapid Response").
- `pytest`: **189 passed**; Ruff lint/format pass.
