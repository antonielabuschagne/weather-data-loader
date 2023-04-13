# Weather Data Loader

I've recently been working with a team where we've had a file processing job to ingest data for some enrichment lookups. The batches of data was fairly small and so we took a serverless approach by using lambda functions to process the data (takes < 15 mins to process). I wanted to illustrate  this with a very basic example where I take a CSV file with geo coordinates (lat/long) to query the weather API to get today's weather.

## Setting up local env

- Copy example.env to .env: `cp example.env .env`
- Edit/populate `.env` with AWS account id
- Deploy infrastructure: `cd cdk && cdk deploy`

## Processing data

* Upload longitude/latitude data to s3 (see [sample](sample.csv))
* Check cloudwatch (log group: weatherapp-onMessageReceivedHandler*) as it should have a log entry with the
  weather data (e.g. `"description": "light rain"`)
