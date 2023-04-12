# psqltobq - incrementally export AlloyDB (PostgreSQL) tables to Big Query

### Wait, what?

This is CronJob to incrementally export AlloyDB tables to Big Query

### Job Requirements/algorithm:
1. For each table get the max(last_updated) from the destination big query table
  ```
  agg_teacher_time_daily
  district_class_course
  student_skill_levels
  window_roster
  course_skill
  agg_student_sat_time_daily
  ```
2. Select all rows from the above tables where `last_updated` is > `max(last_updated)` from Step 1.  Be sure to convert:
    + all UUID fields to text
    + DateTime's that are < 0001-01-01 to be 0001-01-01
    + DateTime's  greater than 9999-12-31 (i.e. window_roster uses ‘infinity’ a special value) to 9999-12-31
4. Insert all these rows into an empty temporary table (truncate the table before starting)
5. Finally, merge the temporary table with the source table i.e.: `join_cols` is the primary key rows
```
        MERGE INTO `{target_table}` T
        USING `{source_table}` S
        ON ({join_cols})
        WHEN MATCHED THEN
            UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN
            INSERT ROW
```

### Implementation plan
1. Write the above job
2.  Create a duplicate copy of the tables `student_skill_levels`, `agg_teacher_time_daily`, and `window_roster` (initially empty but with the same column names/types orders, etc)
3.  Run the job to those test tables 2 days in a row of weekday data ie not Sat and Sunday testing:
    + The job will run in a reasonable time. (the original sql_export_incremental takes 47min daily)
      **Goal**: that this job will not take more than 2hrs ideally
    + The job will not have any memory issues or other unexpected failures
    + The job will correctly merge the `temp_` table into the destination BigQuery export table

### *Notes:*
1. I modified the original `MERGE INTO` SQL such that instead of `INSERT ROW` I used `INSERT (...) VALUES (...)` to account for column order differences between the CSV export, the PostgreSQL/AlloyDB table, and the BigQuery table. Otherwise, it was prone to causing failures (or worse, _succeeding_ at putting the wrong data in the wrong field). For example:
```
MERGE INTO `khanacademy.org:deductive-jet-827`.reports_postgres_exported_temp.agg_student_sat_time_daily T USING `khanacademy.org:deductive-jet-827`.reports_postgres_exported_temp.temp_agg_student_sat_time_daily_20230412_1306 S ON (
  T.as_of_date = S.as_of_date 
  AND T.student_kaid = S.student_kaid 
  AND T.coach_kaid = S.coach_kaid 
  AND T.class_id = S.class_id
) WHEN MATCHED 
AND S.last_updated >= '2023-04-12 13:06:02' THEN 
UPDATE 
SET 
  T.all_ms = S.all_ms, 
  T.district_id = S.district_id, 
  T.school_id = S.school_id, 
  T.last_updated = S.last_updated, 
  T.math_ms = S.math_ms, 
  T.rw_ms = S.rw_ms, 
  T.grade = S.grade WHEN NOT MATCHED THEN INSERT (
    all_ms, district_id, school_id, as_of_date, 
    coach_kaid, class_id, student_kaid, 
    last_updated, math_ms, rw_ms, grade
  ) 
VALUES 
  (
    S.all_ms, S.district_id, S.school_id, 
    S.as_of_date, S.coach_kaid, S.class_id, 
    S.student_kaid, S.last_updated, 
    S.math_ms, S.rw_ms, S.grade
  );
```
2. In testing from my local laptop, this new job takes 32 minutes or less to complete. I expect this to take less time when run in GCP.
3. To export the PostgreSQL / AlloyDB tables I am using `COPY TO` as it is the most efficient method. For example:
```
COPY (
  select * from 
    agg_student_sat_time_daily 
  WHERE 
    last_updated >= '2023-04-12 13:06:02'
) TO STDOUT DELIMITER '^' CSV HEADER
```
4. To protect against minor schema evolution disparities between BigQuery and PostgreSQL / AlloyDB, if a column data type difference exists, a `CAST AS` is attempted. This works for similar data types (e.g. timestamp vs datetime).  If there are the wrong number of columns or major data type differences, the whole job will fail without writing anything to the table.
