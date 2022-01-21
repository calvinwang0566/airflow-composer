echo deploying ...
gsutil -m rsync -r -d dags gs://asia-east2-airflow-prod-6c35af45-bucket/dags
gsutil -m rsync -r -d data gs://asia-east2-airflow-prod-6c35af45-bucket/data
cd dbt
docker build -t asia.gcr.io/loreal-tw/dbt-image:latest .
docker push asia.gcr.io/loreal-tw/dbt-image:latest 