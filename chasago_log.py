from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql import Row

import argparse



# 커맨드라인에서 파라미터를 받습니다.
parser = argparse.ArgumentParser()
parser.add_argument("--path", help="파일 경로의 추가 부분")
parser.add_argument("--token", help="slack token")
args = parser.parse_args()




base_path = "s3://taehun-s3-bucket-230717/"
s3_path = base_path + args.path + "/*"
token = args.token






class ChasagoLog:
    def __init__(self):
        self.spark = SparkSession.builder.appName(f"{args.path}/chasago_log_app").getOrCreate()

        context_page_schema = [StructField("url", StringType(), True)]
        
        context_schema = [
            StructField("userAgent", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("locale", StringType(), True),
            StructField("page", StructType(context_page_schema), True)
        ]

        properties_button_schema = [
            StructField("name", StringType(), True),
            StructField("hospital", ArrayType(StringType()), True)
        ]

        properties_action_schema = [
            StructField("name", StringType(), True),
            StructField("hospital", ArrayType(StringType()), True)
        ]

        properties_properties_schema = [
            StructField("hospitalId", StringType(), True),
            StructField("pageName", StringType(), True),
            StructField("routeName", StringType(), True),
            StructField("to", StringType(), True)
        ]

        properties_schema = [
            StructField("path", StringType(), True),
            StructField("page", StringType(), True),
            StructField("button", StructType(properties_button_schema), True),
            StructField("action", StructType(properties_action_schema), True),
            StructField("properties", StructType(properties_properties_schema), True)
        ]

        overall_schema = StructType([
            StructField("anonymousId", StringType(), True),
            StructField("context", StructType(context_schema), True),
            StructField("messageId", StringType(), True),
            StructField("hospitalId", StringType(), True),
            StructField("properties", StructType(properties_schema), True),
            StructField("event", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("type", StringType(), True),
            StructField("userId", StringType(), True),
            # StructField("partition_0", StringType(), True)
        ])

        self.overall_schema = overall_schema

    def read(self, s3_path):
        df = self.spark.read.option("mergeSchema", "true").schema(self.overall_schema).json(s3_path)
        df = df.cache()
        return df
    
    def add_library(self, path):
        sc = self.spark.sparkContext
        sc.addPyFile(path)


    def process(self, s3_path):
        # 임시
        # partition_0 = s3_path.split('/')[3]
        df = self.read(s3_path)

        df = df.withColumn("timestamp_cov", col("timestamp").cast("timestamp"))\
                .withColumn("utc_timestamp", to_utc_timestamp(col("timestamp_cov"), "UTC"))\
                .withColumn("kst_timestamp", from_utc_timestamp(col("utc_timestamp"), "Asia/Seoul"))\
                .withColumn("add_kst_timestamp", F.date_add(col("kst_timestamp").cast("timestamp"), 1))\
                .withColumn("ywd", F.date_sub(date_trunc("week", col("add_kst_timestamp")), 1))\
                .withColumn("yw", concat(year(col("add_kst_timestamp")), lit("-"), weekofyear(col("add_kst_timestamp")).cast("string")))\
                .withColumn("ym", date_format(col("timestamp"), "yyyy-MM").cast("string"))\
                .withColumn("ymd", date_format(col("timestamp"), "yyyy-MM-dd").cast("string"))\
                # .withColumn("partition_0", lit(partition_0)) # 임시

        df = df.selectExpr(
            "anonymousId",
            "context.userAgent AS context_useragent",
            "context.ip AS context_ip",
            "context.locale AS context_locale",
            "context.page.url AS context_page_url",
            "context",
            "messageId",
            "properties.path AS properties_path",
            "properties.page AS properties_page",
            "properties.button.name AS properties_button_name",
            "properties.action.name AS properties_action_name",
            "properties.action.hospital[0] AS properties_action_hospital_id",
            "properties.action.hospital[1] AS properties_action_hospital_name",
            "properties.button.hospital[0] AS properties_button_hospital_num",
            "properties.button.hospital[1] AS properties_button_hospital_name",
            "properties.properties.hospitalid AS properties_properties_hospitalid", 
            "properties.properties.pagename AS properties_properties_pagename",
            "properties.properties.routename AS properties_properties_routename",
            "properties.properties.to AS properties_properties_to",
            "properties",
            "event",
            "kst_timestamp AS timestamp",
            "yw",
            "ywd",
            "ym",
            "ymd",
            "type",
            "userId"
            # "partition_0"
        )
        
        return df

p = ChasagoLog()
chasago_log = p.process(s3_path)


DL_pk_count = chasago_log.select("messageId").count()
distinct_DL_pk_count = chasago_log.select("messageId").distinct().count()
DL_total_row_count = chasago_log.count() 


save_path = f"s3://taehun-s3-bucket-230717/csv/{args.path}"
test_storage_path =f"s3://taehun-s3-bucket-230717/csv/check_data/{args.path}"


chasago_log.write.json(save_path,mode="overwrite")


loaded_df = p.spark.read.json(save_path)


DW_pk_count = loaded_df.select("messageId").count()
distinct_DW_pk_count = loaded_df.select("messageId").distinct().count()
DW_total_row_count = loaded_df.count() 


# 데이터프레임 형식으로 변환
count_df = p.spark.createDataFrame([
    Row(metric="DL_pk_count", value=DL_pk_count),
    Row(metric="distinct_DL_pk_count", value=distinct_DL_pk_count),
    Row(metric="DL_total_row_count", value=DL_total_row_count),
    Row(metric="DW_pk_count", value=DW_pk_count),
    Row(metric="distinct_DW_pk_count", value=distinct_DW_pk_count),
    Row(metric="DW_total_row_count", value=DW_total_row_count)
])

count_df.write.json(test_storage_path,mode="overwrite")

