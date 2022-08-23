# 建置步驟
## 使用terminal切到
```
cd ./dbt
```

## 安裝virtualenv
```
pip install virtualenv
```

## 使用virtual python env
### create the environment
由於dbt 1.0與python 3.10不相容，因此需要指定venv的python版本
若電腦沒有python 3.9，則需要額外安裝
```
virtualenv --python=python3.9 dbt-env
```
### activate the environment
```
dbt-env\Scripts\activate
```

### install dbt
```
pip install dbt-bigquery==1.0.0
```

## local build需要gcloud權限
```
gcloud auth application-default login
```

## generate doc
```
dbt docs generate --profiles-dir . --target dev
```

# serve doc
```
dbt docs serve --profiles-dir . --target dev
```

# run dbt
```
dbt run --profiles-dir . --models ga_hit_all --target dev
```

## dbt運作方式
- dbt是類似config as code的方式，利用寫出來的code去產生table並且產生關聯
- dbt能夠針對models或tag的方式去執行整個資料流，例如上面的"ga_hit_all"的意思是將整個ga_hit_all相關的models整個建立一次

## 在airflow上使用dbt的方式
- 使用KubernetesPodOperator pull dbt image(包含source code，然後直接在image裡面執行dbt run 
- 為了達成這個目的，就必須在更改dbt版本的時候build一個新的image並且在airflow_utils.py裡面調整 ocker pull的image