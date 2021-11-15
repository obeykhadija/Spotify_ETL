# Spotify ETL
## Data Pipeline Using Spotify's Web API
![logo](https://images.indianexpress.com/2021/08/Spotify.jpg)
## Getting Started
This is the repository contains a data pipeline that downloads a users Spotify data on what songs they've listened to in the last 24 hours, and loads that data in the Db2 database. The pipeline includes a DAG file for automation with Apache Airflow

## Running the Pipeline
Running the app is simple but requires you to have some prereqs:
* Apache Airflow  - you can find instructions to download Apache [here](https://airflow.apache.org/docs/apache-airflow/1.10.12/start.html)
* IBM Cloud Db2 Access - IBM cloud offers Lite plans for free, you can sign up [here](https://cloud.ibm.com/registration/premium1) or if you already have an account simply sign in
* Generate Spotify's API access token [here](https://developer.spotify.com/console/get-recently-played/)

Once you have all the requirements, you should be able to see your DAG in the [Airflow Web UI](http://localhost:8080/) where you can monitor, pause/unpause, your ETL.
