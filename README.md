# Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting  

The project focuses on creating an effective ETL process, starting off with scraping data from reddit, storing and cleaning the data in Excel then applying preprocessing using PySpark, and finally hosting the processed data on AWS S3 Server for further analysis.  

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

<img width="725" alt="2024-03-10" src="https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/d520b88a-0f57-401b-ad0d-1be54355d81b">  

The generated dataframe looks like:  

<img width="737" alt="2024-03-10 (1)" src="https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/0849940c-a862-4852-a217-9ebb204801ed">



Similarly, using python we can achieve the same result in following way:  

```python
import pyspark.sql.functions as func
pythonWay = redditData.filter(redditData.time <= 10)\
            .groupBy("subreddit")\
            .agg(func.sum("upvote").alias("totalUpvotes"))\
            .orderBy("totalUpvotes", ascending=False)

pythonWay.explain()
```

<img width="735" alt="2024-03-10 (2)" src="https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/d50f377f-3f5a-46ff-8a5e-e080a25bfd45">  

Again, the generated dataframe looks like:  

<img width="727" alt="2024-03-10 (3)" src="https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/3ac8ae62-51ad-4216-95aa-bb31d0a17656">


As we can see, the explain plan and generated output are the same for both the approaches!  

## 4. AWS S3 Hosting  

One of the service AWS provides is a simple storage service (S3) which is basic cloud server where data containing text, pictures, etc. can be stored in different file formats. There are several advantages of storing the data on AWS S3 server, which include:  

1. **Scalability and Durability**: S3 scales seamlessly and ensures 99.999999999% durability.

2. **Accessibility**: Simple web services interface for easy retrieval from anywhere.

3. **Reliability**: Redundancy across facilities ensures data availability.

4. **Security**: Robust access control, encryption options for data in transit and at rest.

5. **Cost-Effective**: Pay-as-you-go pricing model based on actual usage.

6. **Data Lifecycle Management**: Automation for efficient data movement and deletion.

7. **Integration**: Seamless integration with other AWS services.

8. **Global Reach**: Multiple data centers globally for low-latency access.

In summary, S3 simplifies storage, enhances accessibility, and provides a reliable, scalable cloud solution.  

Let's look at how to upload files to AWS S3 server using python environmnet.  

First, create a AWS user account, and then from services select S3.  

<img width="956" alt="2024-03-10 (4)" src="https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/74322446-e5b5-4424-945e-784fcf55d537">  

Create a new S3 bucket by following and understanding the instructions, once the bucket is created, it might look something like this:  

<img width="962" alt="2024-03-10 (6)" src="https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/e1e0912e-a7ba-4c6b-9eef-b0f16e508814">  

Here in myredditbucket I have created a folder named redditData in which we will store our file that we previously generated. We will use following python code to upload the file to AWS S3 bucket from python environment. We are going to upload csv version of our output to AWS S3 server, hence let's convert the pyspark dataframe to a csv file first.

```python
pythonWay.toPandas().to_csv('subreddit.csv')
```
We will use the boto3 library to upload the csv file to our aws s3 bucket.  

```python
import boto3

session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='',
)

s3 = session.resource('s3')

s3.meta.client.upload_file('subreddit.csv', 'myredditbucket', 'redditData/subreddits.csv')
```
Which upload the subreddits.csv on our S3 server.  

<img width="962" alt="2024-03-10 (7)" src="https://github.com/jugal-chauhan04/Reddit-Data-Pipeline-Scraping-Cleaning-and-Cloud-Hosting/assets/111266884/eda0a12c-07df-4f33-95bd-209589bba602">  

## 5. Conclusion  

In summary, the Reddit Data Pipeline project accomplished its goal of efficiently gathering insights from Reddit data. We started by scraping data with Scrapy, cleaned it in Excel, and then processed it using PySpark. The AWS S3 hosting added a reliable cloud storage solution.

This project showcased the power of PySpark in analyzing data, emphasizing two approaches. The integration with AWS S3 provided scalability, accessibility, and security benefits.

In practical terms, this pipeline demonstrates how to handle web data effectively, making it a valuable resource for those seeking insights from Reddit and similar platforms.















