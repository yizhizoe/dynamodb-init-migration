# DynamoDB Migration with Filtering

## Background

- Migration of DynamoDB table from AWS global region to China regions
- [DynamoDB cross region replication](https://github.com/aws-samples/aws-dynamodb-cross-region-replication) provides a nice example of continous bi-directional replication but doesn't cover the inital migration. This PoC can be combined with the former as a total solution for DynamoDB migration and replication. 
- Due to data privacy law, **filtering** on the tables is required before migration
- Internet network is not stable and the migration step over Internet requires reliable architecture

## Architecture

Filter and export to S3 and replicate to China

- Using Glue, crawl and filter DynamoDB table to export to S3 in global region
- Replicate to S3 in China using S3 Plugin of [Data Transfer Hub](https://www.amazonaws.cn/en/solutions/data-transfer-hub/
  )
- Use Glue to load data from S3 to DynamoDB

![image-20211025105443432](img/image-20211025105443432.png)

## Setup

Setup US region (source table filtering to S3)

Setup Glue Crawler on US region to crawl over source DynamoDB table

![image-20211026160043023](img/image-20211026160043023.png)