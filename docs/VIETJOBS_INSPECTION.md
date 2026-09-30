# VietJobs dataset inspection

## Scope and provenance

On 2026-09-30, the complete published [`dinhieufam/VietJobs` dataset](https://huggingface.co/datasets/dinhieufam/VietJobs) was downloaded from `main/VietJobs.csv` into ignored local `data/VietJobs.csv`. It has **48,092 rows**, is **103,110,903 bytes**, and has SHA-256 `85862b06fda4e814fe0c1d8622f173d189c92758345f77df16d1232d0c49d477` (matching the publisher's LFS object ID). Findings below are from the full CSV unless labeled otherwise. The small `tests/fixtures/vietjobs_small.csv` fixture is only for offline tests. The full CSV must remain uncommitted.

Profiling used Python's `csv.DictReader` with UTF-8-SIG decoding. A null means an empty or whitespace-only CSV cell; no literal `null`, `none` or `nan` cells were observed. Duplicate counts use exact parsed CSV values. The CSV has no typed schema: all cells arrive as strings, but `salary_min` and `salary_max` parse as integers in every row.

| Column | Inferred type | Empty | Empty % | Distinct nonempty |
| --- | --- | ---: | ---: | ---: |
| `job_title` | text | 0 | 0.00% | 16,720 |
| `location` | text | 0 | 0.00% | 984 |
| `country` | text | 0 | 0.00% | 10 |
| `qualifications` | text, often serialized list | 370 | 0.77% | 18,167 |
| `technical_skills` | text, often serialized list | 6,437 | 13.38% | 26,426 |
| `soft_skills` | text, often serialized list | 2,641 | 5.49% | 31,790 |
| `languages_required` | text | 35,796 | 74.43% | 123 |
| `experience_required` | text | 0 | 0.00% | 19 |
| `salary` | text | 0 | 0.00% | 839 |
| `contract_type` | text | 0 | 0.00% | 9 |
| `working_hours` | text | 4,982 | 10.36% | 9,786 |
| `benefits` | text, often serialized list | 39 | 0.08% | 36,555 |
| `description` | text | 0 | 0.00% | 34,555 |
| `requirements_text` | text | 9 | 0.02% | 35,326 |
| `category` | text | 0 | 0.00% | 16 |
| `salary_min` | integer-like CSV text | 0 | 0.00% | 57 |
| `salary_max` | integer-like CSV text | 0 | 0.00% | 69 |
| `salary_avg` | text, e.g. `12 triệu`, `Thoả thuận` | 0 | 0.00% | 88 |

The file has **385 exact duplicate rows beyond the first occurrence** across **381 duplicate groups** (maximum group size 3). There are **31,372 repeated titles beyond the first** across 7,275 title groups, and **12,302 repeated `(job_title, description)` pairs beyond the first** across 9,038 pair groups. Repeated titles or pairs are not proof of duplicate postings because company, posting ID, URL and posting date are absent. The maximum title group has 1,041 rows; the maximum title/description group has 58.

`salary_min=0` occurs in 13,775 rows and `salary_max=0` in 13,732. Both are zero in 13,730 rows, of which 13,727 have `salary_avg=Thoả thuận`. Zero is therefore not a trustworthy salary amount. The numeric columns have no currency or unit field; retain raw salary text rather than inferring a unit.

## Distributions

| Exact `category` value | Rows | Share |
| --- | ---: | ---: |
| `kinh_doanh_bán_hàng_chăm_sóc_khách_hàng` | 8,276 | 17.21% |
| `sản_xuất_lao_động_phổ_thông_cơ_khí` | 6,407 | 13.32% |
| `marketing_truyền_thông_quảng_cáo_nội_dung` | 5,950 | 12.37% |
| `tài_chính_kế_toán_ngân_hàng_bảo_hiểm` | 5,530 | 11.50% |
| `du_lịch_nhà_hàng_khách_sạn_dịch_vụ` | 4,243 | 8.82% |
| `thiết_kế_nghệ_thuật_giải_trí_truyền_hình_báo_chí` | 3,465 | 7.20% |
| `nhân_sự_hành_chính_pháp_chế_tư_vấn` | 3,276 | 6.81% |
| `xây_dựng_kiến_trúc_bất_động_sản` | 2,811 | 5.85% |
| `công_nghệ_thông_tin_kỹ_thuật_số` | 1,906 | 3.96% |
| `logistics_vận_tải_chuỗi_cung_ứng` | 1,813 | 3.77% |
| `kỹ_thuật_điện_điện_tử_viễn_thông` | 1,236 | 2.57% |
| `giáo_dục_đào_tạo_nghiên_cứu` | 1,165 | 2.42% |
| `y_tế_dược_chăm_sóc_sức_khỏe_công_nghệ_sinh_học` | 963 | 2.00% |
| `ngôn_ngữ_dịch_thuật` | 384 | 0.80% |
| `nhóm_nghề_khác` | 345 | 0.72% |
| `nông_nghiệp_năng_lượng_môi_trường` | 322 | 0.67% |

The primary technology category contains **1,906 / 48,092 (3.96%)** records. It is named `công_nghệ_thông_tin_kỹ_thuật_số`, not `Technology`.

| `contract_type` | Rows |
| --- | ---: |
| `Toàn thời gian` | 46,058 |
| `Bán thời gian` | 874 |
| `Thực tập` | 822 |
| `Khác` | 165 |
| `Thời vụ` | 92 |
| `Cộng tác viên` | 54 |
| `Toàn thời gian, Bán thời gian` | 16 |
| `Toàn thời gian, Thực tập` | 10 |
| `Thời vụ, Cộng tác viên` | 1 |

`location` has 984 exact values, including districts and multi-city values. The ten most common are `hà nội` 18,139; `hồ chí minh` 15,311; `hà đông` 1,180; `bắc ninh` 938; `đà nẵng` 931; `hải phòng` 912; `hà nội, hồ chí minh` 665; `hưng yên` 655; `đồng nai` 553; `tây ninh` 475. These are source strings, not normalized geography.

| `experience_required` | Rows | `experience_required` | Rows |
| --- | ---: | --- | ---: |
| `1 năm` | 15,372 | `Không yêu cầu` | 12,345 |
| `2 năm` | 9,781 | `3 năm` | 5,595 |
| `6 tháng` | 2,258 | `5 năm` | 1,934 |
| `4 năm` | 399 | `3 tháng` | 142 |
| `7 năm` | 91 | `10 năm` | 64 |
| `8 năm` | 34 | `6 năm` | 30 |
| `18 tháng` | 19 | `2 tháng` | 10 |
| `9 tháng` | 7 | `30 tháng` | 5 |
| `15 năm` | 3 | `12 năm` | 2 |
| `8 tháng` | 1 | | |

## Technology role coverage and filtering implications

The structured technology category is a high-precision starting point, but it misses clear target jobs assigned to other categories. Exact title-substring probes found 14 of 17 `data scientist` rows, 8 of 40 `devops`, 8 of 49 `tester`, 3 of 12 `data engineer`, 2 of 31 `frontend`, and 1 of 4 `database` outside that category. Examples include `Data Scientist` under construction, `DevOps Engineer - Vận Hành ERP Odoo, FastApi` under sales, `Manual Tester [Dự Án Banking]` and `Database Administrator` under finance, and `Senior Frontend Developer` under finance. Probe counts overlap and are not an aggregate filter result.

The implemented `is_technology_job` filter retains **2,043 / 48,092 rows (4.25%)** from this full CSV: **1,906** through the exact category rule and **137** through its current title fallback. The fallback recognizes explicit software-development, data science/engineering, AI/ML, DevOps/SRE, software testing, cybersecurity and database role phrases. For non-IT categories, it rejects title matches containing marketing, MKT, admissions, teaching or presales terms. This is the count for the current deterministic rule, not an independently verified ground-truth count of all technology jobs. Filtering is separate from acquisition so raw source records remain available for later audit or rule changes.

The title fallback must be narrow. A bare `QA` substring matched 184 titles, mostly manufacturing and construction quality roles. Bare `cloud` matched sales roles such as `Trưởng Ban Kinh Doanh Google Cloud Platform (GCP)` and `Chuyên Viên Tư Vấn Cloud (Cloud Presales)`. Bare `fullstack` included `Thực Tập Sinh Marketing/ Digital MKT Fullstack`. Filtering on description words such as “data”, “system”, “software” or “technology” would be much broader and less defensible. Some relevant jobs may still be missed when a title uses an unlisted synonym; some title matches in mislabeled categories may still need human review. The source's IT category can itself include roles outside the narrower software/data/AI product focus, so the 2,043 figure measures the rule, not perfect precision.

## Fields needed by AI Career Platform

Representative records were examined for backend, frontend, full stack, software engineering, data engineering, data science, AI, DevOps, software testing, cybersecurity and database engineering. All roles were present. Examples:

| Role | Example title | Distinct information |
| --- | --- | --- |
| Backend | `Backend Developer (Nodejs)` | Description covers duties; requirements add degree and 1–3 years; technical list names Node.js, Express, NestJS, MySQL and CassandraDB. |
| Frontend | `Kỹ sư Frontend cao cấp (React/NextJS)` | Requirements add education/experience; technical list names ReactJS, NextJS, JavaScript and tooling. |
| Data engineering | `Data Engineer` | Description covers ETL work; requirements add degree and years; technical list names ODI, Airflow and SQL/PLSQL. |
| AI | `Kỹ Sư Trí Tuệ Nhân Tạo` | Description is broad context; requirements and technical list give specific skills and qualifications. |
| QA | `Software Test (Tester, QA, ERP)` | Requirements specify education and years; technical list includes Selenium and test artifacts. |
| Security | `Trưởng Nhóm An Ninh Mạng/ Cyber Security Lead` | Description is only 86 characters; requirements and technical list carry substantive security criteria. |
| Database | `Database Engineer (DBA)` | Requirements add DBMS experience; technical list names MySQL, MariaDB, PostgreSQL, Oracle and SQL tuning. |

The fields are **complementary with some semantic overlap**, not interchangeable or mostly exact duplicates. Across all 48,092 rows, `requirements_text` is nonempty in 48,083; after case folding and whitespace normalization, it equals `description` in only 7 and is fully contained in it in only 58. Within the 1,906 technology-category rows, requirements are nonempty in all 1,906 and are neither equal to nor contained in description in any row. `technical_skills` is nonempty in 1,804/1,906; `qualifications` in 1,901/1,906; `experience_required` in all 1,906. The skills and qualifications columns often resemble serialized lists derived from prose, while experience is a concise field that may coexist with a more nuanced requirement in prose. String inequality does not prove semantic independence, but description alone plainly loses useful extraction input. Preserve source values verbatim; AI Career Platform decides required/preferred skills and experience semantics.

## Canonical mapping implications

- `job_title` → `raw_title`; `location` → `raw_location`; `contract_type` → `raw_employment_type`.
- No company, source ID, URL or posting date column exists; map these to `None` rather than fabricate values.
- Preserve `description` plus nonempty `requirements_text`, `experience_required`, `technical_skills` and `qualifications` in deterministically labeled `raw_description` sections so downstream extraction can see them. Avoid repeating source values that are identical or trivially repeated; do not summarize or infer skills in DE.
- Preserve all original columns in later raw/Bronze storage for audit, including salary, benefits and working hours. Hash the complete parsed source row with sorted keys, including fields outside the canonical model, while excluding collection time, local path and row position. Cell values are not normalized for the hash. Identical rows then share a hash across reruns; a changed source field changes it. This is content identity, not a stable posting ID.

## Small fixture distinction

The previously inspected five-row publisher sample had 3 blank `technical_skills`, 5 blank `languages_required`, 0 exact duplicate rows and 0 duplicate title/description pairs. Those figures describe only the sample, not the full dataset. The committed test fixture is also small and used only for offline tests.
