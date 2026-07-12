#!/usr/bin/bash

sudo docker compose exec airflow-scheduler airflow dags unpause movielens_silver_pipeline
sudo docker compose exec airflow-scheduler airflow dags trigger movielens_silver_pipeline
