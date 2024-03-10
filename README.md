# Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting  

The project focuses on creating an effective ETL process, starting off with scraping data from reddit, storing and cleaning the data in Excel then applying preprocessing using PySpark, nad finally hosting the processed data on AWS S3 Server for further analysis.  

## 1. Extracting Data by Scraping Reddit using Scrapy  

Python libraries like BeautifulSoup and Scrapy provide an effective method to scrap the web and gather data, for this project I learned and applied Scrapy library to collect data from reddit's popular page. I organized the data into title, subreddit, upvotes, and time posted columns with corresponding rows and generated the results in a .csv file.  

![image](https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/8a8532a9-11c5-4cd1-9176-27269b9d7cdf)


## 2. Data Cleaning in Excel  

The raw data obtained from web srcaping is almost never clean, and needs to be investigated for appropriate data types and other data integrity standards. To achieve this, I browsed the data using excel's filter and sort options and recognized few problems.  

![image](https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/cf9b16a2-a66f-40bc-a8a7-e018ee7a9d41)  

The upvote colum did not have a consistent format for the amount of upvotes, posts containing thousands of upvotes were displayed as 26.7k, to make the column fill with consistent values, I utilized the search and replace features of excel. Additionally, I got rid of the 'hours ago' text in the time column so that it can be converted into number type column. This much data cleaning was enough for excel's feature, and I decided to do the rest in PySpark.  

## 3. Data Processing using PySpark  

When loading data into a pyspark session, spark's engine automatically determines the data type for column values. However, in most practical applications, it is better to assign schema manually to avoid any mistakes in our data. We can achieve this using following script.  

```python
# Creating Manual Schema

from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType

myManualSchema = StructType([
StructField("id", IntegerType(), True),
StructField("title", StringType(), True),
StructField("subreddit", StringType(), True),
StructField("upvote", LongType(), True),
StructField("time", IntegerType(), True)
])
```

Now, use our manual schema to load the data.  

```python
df = spark.read.format("csv").schema(myManualSchema).load("oldreddit.csv")
df.printSchema()
```
What we aim to do here is, try to generate another spark dataframe that will contain subreddits with most upvotes in last 10 hours. This can be achieved in multiple ways in PySpark, we will showcase two approaches here and also display Spark's lazy evaluation technique.  

The first approach is using the sparksql and apply an SQL query to obtain the result.

```python
# Create a view for our data to use Sql
redditData.createOrReplaceTempView("redditData")

# Using Sql Query
sqlWay = spark.sql('''
SELECT subreddit, SUM(upvote) AS totalUpvotes
FROM redditData
WHERE time <= 10
GROUP BY subreddit
ORDER BY totalUpvotes desc;
''')

sqlWay.explain()
```  






