# Airflow Data Pipelines

Forth project provided by Udacity in the "Data Engineering with AWS" course, the main purpose of which is to practice the core concepts of Apache Airflow. 

Below is provided an example DAG for the project: 

![image](https://github.com/user-attachments/assets/e23fa74e-2a41-43df-9e30-eb29f55193e3)

 The template provided contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline, as well as four empty operators that need to be implemented into functional pieces of a data pipeline. 

 Locations for the datasets for the project:

- Log data: *s3://udacity-dend/log_data*
- Song data: *s3://udacity-dend/song-data*

Copy the data from the udacity bucket to the home cloudshell directory:

```bash
aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
aws s3 cp s3://udacity-dend/log_json_path.json ~/
```
Copy the data from the home cloudshell directory to your own bucket -- this is only an example:

```bash
aws s3 cp ~/log-data/ s3://kgolovko-data-pipelines/log-data/ --recursive
aws s3 cp ~/song-data/ s3://kgolovko-data-pipelines/song-data/ --recursive
aws s3 cp ~/log_json_path.json s3://kgolovko-data-pipelines/
```
List the data in your own bucket to be sure it copied over:

```bash
aws s3 ls s3://kgolovko-data-pipelines/log-data/
```
To check issues during the loading process in Redshift, use the below command in Redshift editor (connect using the user and password during the Redshift creation):

```bash
SELECT * 
FROM SYS_LOAD_ERROR_DETAIL
ORDER BY start_time DESC
LIMIT 10;
```
