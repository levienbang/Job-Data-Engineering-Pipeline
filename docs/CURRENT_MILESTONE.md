# Current milestone — không có

> Hiện **không có milestone đang làm**. Milestone 8 (Delivery-ready), 8.1 (dọn
> luồng acquisition) và **8.2 (Docker Compose PostgreSQL + Airflow, chạy DAG thật)**
> đã hoàn thành và nằm trên `main`. **Không implement lại gì từ file này.**
> Milestone 9 Kafka/Snowflake trong `docs/PROJECT_SPEC.md` **không** được yêu cầu.
> Khi có việc mới, user sẽ ghi đè file này bằng milestone mới.

M8.2: build/pip check thành công; PostgreSQL và Airflow healthy, init exit 0;
DAG sample chạy hai lần 8/8 task success. Sample có sẵn bị lọc IT toàn bộ, nên
đã chạy thêm subset IT thật hai lần: Gold 5 → 5, duplicate_count 0 → 5,
Bronze checksum/mtime không đổi. Metadata runs succeeded, DAG giữ paused,
delivery tắt. Tests hiện có 93 passed, Ruff lint/format pass. Không đổi logic
`src/job_pipeline/` hoặc repo `../LLM`. Chi tiết trong README mục Docker.
