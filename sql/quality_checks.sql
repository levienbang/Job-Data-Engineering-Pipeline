-- Replace the path with a local Gold Parquet file before running in DuckDB.
CREATE VIEW jobs AS SELECT * FROM read_parquet('data/gold/source=vietjobs/jobs.parquet');
SELECT count(*) AS job_count,
       count(*) FILTER (WHERE raw_title IS NULL OR trim(raw_title) = '') AS missing_title,
       count(*) FILTER (WHERE raw_description IS NULL OR trim(raw_description) = '') AS missing_description
FROM jobs;
SELECT source_name, coalesce('id:' || source_job_id, 'hash:' || source_record_hash) AS identity,
       count(*) AS copies
FROM jobs GROUP BY 1, 2 HAVING count(*) > 1;
SELECT raw_location, count(*) AS jobs FROM jobs GROUP BY 1 ORDER BY jobs DESC LIMIT 10;
SELECT raw_employment_type, count(*) AS jobs
FROM jobs GROUP BY 1 ORDER BY jobs DESC;
SELECT count(*) FILTER (WHERE source_job_id IS NULL) AS missing_source_id,
       count(*) FILTER (WHERE source_url IS NULL) AS missing_url,
       count(*) FILTER (WHERE raw_company IS NULL) AS missing_company,
       count(*) FILTER (WHERE raw_location IS NULL) AS missing_location,
       count(*) FILTER (WHERE posted_at IS NULL) AS missing_posted_at,
       count(*) FILTER (WHERE raw_requirements IS NULL) AS missing_requirements,
       count(*) FILTER (WHERE raw_experience IS NULL) AS missing_experience,
       count(*) FILTER (WHERE raw_technical_skills IS NULL) AS missing_technical_skills,
       count(*) FILTER (WHERE raw_soft_skills IS NULL) AS missing_soft_skills,
       count(*) FILTER (WHERE raw_qualifications IS NULL) AS missing_qualifications,
       count(*) FILTER (WHERE raw_languages IS NULL) AS missing_languages,
       count(*) FILTER (WHERE raw_benefits IS NULL) AS missing_benefits,
       count(*) FILTER (WHERE raw_salary IS NULL) AS missing_salary,
       count(*) FILTER (WHERE raw_working_hours IS NULL) AS missing_working_hours,
       count(*) FILTER (WHERE raw_category IS NULL) AS missing_category,
       count(*) FILTER (WHERE raw_country IS NULL) AS missing_country
FROM jobs;
