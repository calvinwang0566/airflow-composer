call echo deploying ...
call gsutil -m rsync -r -d ./dags gs://asia-east2-airflow-prod-6c35af45-bucket/dags
call powershell "gcloud container images list-tags asia.gcr.io/loreal-tw/dbt-image --filter='-tags:*' --format='get(digest)' --limit=unlimited | ForEach-Object { gcloud container images delete "asia.gcr.io/loreal-tw/dbt-image@$PSItem" --quiet }"
call gcloud builds submit --tag asia.gcr.io/loreal-tw/dbt-image:latest ./dbt