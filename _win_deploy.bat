call echo deploying ...
call gsutil -m rsync -r -d dags gs://asia-east2-airflow-prod-6c35af45-bucket/dags
call gsutil -m rsync -r -d data gs://asia-east2-airflow-prod-6c35af45-bucket/data
call docker build -t asia.gcr.io/loreal-tw/dbt-image:latest .
call docker push asia.gcr.io/loreal-tw/dbt-image:latest 