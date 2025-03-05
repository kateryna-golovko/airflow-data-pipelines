# Airflow Data Pipelines

Forth project provided by Udacity in the "Data Engineering with AWS" course, the main purpose of which is to practice the core concepts of Apache Airflow. 

Below is provided an example DAG for the project: 

![image](https://github.com/user-attachments/assets/e23fa74e-2a41-43df-9e30-eb29f55193e3)

 The template provided contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline, as well as four empty operators that need to be implemented into functional pieces of a data pipeline. 

 Locations for the datasets for the project:

Log data: *s3://udacity-dend/log_data*
Song data: *s3://udacity-dend/song-data*

Start from copying the data into your own S3 bucket from the same region:

*aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
aws s3 cp s3://udacity-dend/log_json_path.json ~/*



