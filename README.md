# flightsim-intelligence
A personal playground/training project using:
![syss](https://user-images.githubusercontent.com/42417723/132095621-7bf62656-3b37-4dd7-a95c-c55417a2ddaa.png)


A flightsim intelligence service developed by Alexander Viala Bellander.

The system collects telemetry and server status data from Flightsim Network Operators for business intellegence and educational purposes. Intelligence reports were to be created weekly.

This project is not commercial and was created as a learning project on how to establish certain cloud infrastructure and minimise cost.

## system architecture
![Architecture](architecture.png)

## database cost
With `INSERT` operations occuring every hour `0 * * * *` and a `AUTO_SUSPENSION = 180` the daily operating cost amounted to ~1.30 credits.
Without analysis and storage costs this amounts to a monthly operating cost of ~39 credits or ~85 EUR

## learnings and proposed changes
I realised that what we would have liked is to use snowflake staging and pipelines to reduce cost. Instead of inserting data hourly we could upload the json documents to a Google Cloud Storage Bucket and use it as a stage environment in snowflake. Then have a pipeline operating on the documents weekly.

Similarly we could upload the documents to GCS (or other cloud storage) and run a Cloud Function (FaaS) that fetches the data in the bucket and inserts it to the database weekly.

These options would likely reduce operating costs since storage is much less expensive than having to operate the database hourly.


## analysis results
![image](https://user-images.githubusercontent.com/42417723/132095305-57c4742c-2c98-439d-9ed4-5fd805cfafb9.png)
![image](https://user-images.githubusercontent.com/42417723/132095311-de50cf72-3a3d-439d-a70a-9bc0cbd94701.png)
![image](https://user-images.githubusercontent.com/42417723/132095319-6b774655-d080-440a-9577-cd3aea40dc01.png)

## copyright
Copyright Alexander Viala Bellander 2021 © 
All rights reserved.
