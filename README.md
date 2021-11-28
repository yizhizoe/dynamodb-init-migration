# DynamoDB Table Initial Migration across AWS Partitions with Filtering

## Background

- Initial migration of DynamoDB table from AWS global region to China regions (as AWS China regions are separated from AWS commercial parition, aka global regions, the common practice of DynamoDB global table is not available for cross-paritition migration)
- [DynamoDB cross region replication](https://github.com/aws-samples/aws-dynamodb-cross-region-replication) provides a nice example of continous bi-directional replication but doesn't cover the inital migration. This PoC can be combined with the former as a total solution for DynamoDB migration and replication. 
- Due to data privacy law, **filtering** on the tables is required <u>*before*</u> migration
- Cross-border Internet network is not stable and the migration step over Internet requires reliable architecture

## Architecture

Filter and export to S3 and replicate to China

- Using Glue, crawl and filter DynamoDB table to export to S3 in global region
- Replicate to S3 in China using S3 Plugin of [Data Transfer Hub](https://www.amazonaws.cn/en/solutions/data-transfer-hub/
  )
- Use Glue to load data from S3 to DynamoDB

![image-20211025105443432](img/image-20211025105443432.png)

## Setup

#### A. Setup US region (source table filtering to S3)

We will use the sample data that's generated from the [DynamoDB cross region replication](https://github.com/aws-samples/aws-dynamodb-cross-region-replication) which is a fake user profile table where in every item of the table, there is field "country". The Glue job will filter on this field. 

1. Glue Crawler

Setup Glue Crawler on US region to crawl over source DynamoDB table

![image-20211026160043023](img/image-20211026160043023.png)

The catalog will be as below

<img src="img/image-20211026164056460.png" alt="image-20211026164056460" style="zoom:50%;" />

2. Upload ETL script

```bash
git clone https://github.com/yizhizoe/dynamodb-init-migration.git
aws s3 cp source_ddb_filter.py s3://aws-glue-scripts-{account_id}-us-west-2/ --region us-west-2
```

3. Set up and run Glue ETL job

Create Glue job as below and specify the script S3 path to "s3://aws-glue-scripts-{account_id}-us-west-2/source_ddb_filter.py"

![image-20211026172241060](img/image-20211026172241060.png)

In Job parameter, input the key "export_s3_bucket" and the bucket for export in US region. Set the appropriate worker number and use Glue 2.0

![image-20211026174117811](img/image-20211026174117811.png)

After creating the job, run the job directly.

#### B. Set up Data Transfer Hub

Follow the [deployment guide](https://github.com/awslabs/amazon-s3-data-replication-hub-plugin/blob/main/docs/DEPLOYMENT_EN.md) to set up Data Transfer Hub. Add the replication from the source <export_s3_path> to the s3 path <transfer_target_s3_path> in China region. The Data Transfer Hub transfers Amazon S3 objects between AWS China regions and Global regions has auto retry mechanism and error handling so as to provide high resiliency in data transfer over Internet. As it also supports incremental data transfer, the setup for s3 replication can be one-time setup and you can reuse the export path for multiple tables replication. 

#### C. Setup in China regions

1. Set up Glue crawler in China region to crawl over the s3 target <transfer_target_s3_path>. The role should have both AWSGlueServicePolicy and access to the S3 target path.

   <img src="img/image-20211121225626681.png" alt="image-20211121225626681" style="zoom:33%;" />

2. The catalog should be similar to the one in US region. 

   ![image-20211121225850480](img/image-20211121225850480.png)

3. Create the target DynamoDB table user_migrated_cn with the same Partition key and Sort Key as in US region. Set the Capacity mode to "**On-demand**". 

   <img src="img/image-20211121230907702.png" alt="image-20211121230907702" style="zoom:33%;" />

4. Upload ETL script

```bash
aws s3 cp dump_target_ddb.py s3://aws-glue-scripts-{account_id}-cn-north-1/ --region cn-north-1
```

5. Set up and run Glue ETL job

   Create Glue job as below and specify the script S3 path to "s3://aws-glue-scripts-{account_id}-cn-north-1/dump_target_ddb.py"

<img src="img/image-20211128225456024.png" alt="image-20211128225456024" style="zoom:33%;" />

​	In Job parameters, add "--target_ddb_table_name=user_migrated_cn"

![image-20211128225632937](img/image-20211128225632937.png)

6. Save and run the job. Note that Glue crawler is case insensitive so in this step, it's *important* to double check on the target item attribute names, e.g. we deliberated mapped "pk" to "PK" in the target table. After the job is finished, go to DynamoDB table "user_migrated_cn" and verify that the items are created and they all have attribute "country"= "China".  

   ![image-20211128232815209](img/image-20211128232815209.png)

   To further verify the item number, run "Get live item count".

   <img src="img/image-20211128232545512.png" alt="image-20211128232545512" style="zoom: 33%;" />

