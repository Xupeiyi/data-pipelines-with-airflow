import json
import pathlib

import requests
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.date_ago(14),
    schedule_interval=None 
)


def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
 
    # get image urls from launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]

    # download pictures from image urls
    for image_url in image_urls:
    
        try:
            response = requests.get(image_url)
            image_filename = image_url.split("/")[-1]
            target_file = f"/tmp/images/{image_filename}"

            with open(target_file, "wb") as f:
                f.write(response.content)
            
            print(f"Downloaded {image_url} to {target_file}")

        except requests.exceptions.MissingSchema:
            print(f"{image_url} appears to be an invalid URL.")

        except requests.exceptions.ConnectionError:
            print(f"Could not connect to {image_url}.")
 
 
