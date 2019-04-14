# Google Cloud Function Fixed Width File Streaming into BQ

This is a Google Cloud Function that processes tens of thousands of tiny fixed width files and performs streaming inserts into BQ.

The function is triggered by each file finalize/create in a Google Cloud Storage bucket.

Due to the tiny size of the file (less than 10KB) we are able to use a Google Cloud Functions to do all of the processing required without the need for more scalable data processing tools like dataflow or dataproc.

When a file lands in the GCS bucket, the function reads the file into memory, parses the file by position into columns, adds a new DataDate column, performs various data type and format cleanup, converts the dataframe to numpy array, and then loads the records into BigQuery via streaming inserts.

I have used this simple automated serverless data pipeline to process 200,000+ files per day into BQ without issue to support  real time operational dashboards.

![Image of Architecture](https://raw.githubusercontent.com/alanjbates/Google-Cloud-Function-Fixed-Width-Stream-to-BQ/master/fixed_width_streaming_to_bq.draw.io.png)
