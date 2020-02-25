
# JourneyWiser

This project builds a data pipeline to create a platform for predicting the booking lead time for a given airbnb listing and travel date. Booking lead time is a metric defined as the number of days between reservation date and check-in date. All the processed data is stored in Postgresql, queried in flask and visualized by a plug-in installed on chrome browser.
The lead time prediction is made by ingesting and analyzing 480 GB of historical data from airbnb collected for the past few years.

[link to slides](https://docs.google.com/presentation/d/1vo_jyTEAO1pe561yQhm0KKI3HU9puxuBQUplZ-Yy1w0/edit#slide=id.g6e15d5f2f7_0_126)

<p align="center">
    <img src = "./images/plugin.png" class = "center" width="500">
</p>

## Pipeline Architecture
The raw data were uncompressed and uploaded to s3 in csv format. The data was then batch processed in spark and the processed data was fed to postgresql. Finally the data was visualized using flak and chrome plogin.
![image description](images/pipeline.png)

## Dataset
470GB data provided by [insideairbnb.com](http://insideairbnb.com/get-the-data.html).

## Performance Optimizations
### S3 Access
Many S3 access requests degrades the batch processing efficiency. The issue was resolved by fetching the data required for each city into a dictionary consist of spark dataframes. 

### Batching Performance Optimization
Considering Spark's lazy evaluation, the batching performance was improved by parallelizing batch processing algorithm in order to minimize triggering spark actions and thus take the most advantage out of spark cluster computing.

To achieve this, for each city all the dataframes were merged to a single dataframe and the scrape date of each was added as a new column. After filtering, the data were grouped by date and listing id and followed by aggragation (minimum leadtime value). Finally all the processed data was written to database at once.

```
df_all = reduce(DataFrame.unionAll, dfs)
df_all.withColumn('lead_time',when(df_all['available'] == 't', datediff(df_all['date'],df_all['scrape_date']))
df_all.groupBy('date','listing_id').agg({'lead_time': 'min'})
df_city.write.format('jdbc')...
```
![image description](images/spark_optimization.png)

### Database Query Optimization
Due to the large amount of data stored in database, queries could take a long time to process. To resolve the issue the table containing the lead time values was sorted and indexed based on listing ids.

```
CREATE INDEX listingid_asc ON leadtime_history (listing_id ASC);
```

