setting-airflow:
	mkdir -p ./dags ./logs ./plugins ./config ./data 

install:
	python -m pip install --upgrade pip &&\
		pip install -r requirements.txt
		
jdbc-postgresql:
	wget https://jdbc.postgresql.org/download/postgresql-42.2.5.jar -P ./pyspark-data/
