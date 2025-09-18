## Summary

Please provide a brief, high-level summary of the changes in this pull request. What is the goal of this work?

---

## Related Issue

* Closes # (issue number)

---

## Acceptance Criteria

* [ ] The `ingest_stocks` DAG successfully fetches data for the new ticker.
* [ ] The `load_stocks` DAG correctly loads the new data into the `alpha_vantage_daily` table.
* [ ] The `dbt_run_models` DAG successfully transforms the new data.

---

## How to Test

Please provide clear, step-by-step instructions on how to test these changes.

1. Run the `stocks_ingest_controller` DAG.
2. Verify that a new file for the ticker appears in the Minio 'test' bucket.
3. Connect to the `postgres_dwh` database and confirm the new data is in the `alpha_vantage_daily` table and the `src_alphavantage_daily` view.

---

## Screenshots

Please provide any relevant screenshots of the successful pipeline run in the Airflow UI, or of the final data in the database.
