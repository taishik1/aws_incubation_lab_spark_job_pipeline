#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import hashlib

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, concat_ws, sha2
from pyspark.sql.types import *

# In[ ]:


spark = (
    SparkSession.builder.appName("transformation")
    .config("s3://taishik-landingzone-batch08/Configuration/delta-core_2.12-0.8.0.jar")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.addPyFile(
    "s3://taishik-landingzone-batch08/Configuration/delta-core_2.12-0.8.0.jar"
)


# In[ ]:


from delta import *

# In[ ]:


def create_lookup_table(lookup_path, src_df, dataset):
    if dataset == "actives":
        piicolumns = ["advertising_id", "user_id", "masked_advertising_id", "masked_user_id"]
        src_df = src_df.select([col for col in piicolumns])
        try:
            # to get the lookup table
            delta_table = DeltaTable.forPath(spark, lookup_path)
        except:
            lookup_schema = StructType(
                [
                    StructField("advertising_id", StringType(), True),
                    StructField("user_id", StringType(), True),
                    StructField("masked_advertising_id", StringType(), True),
                    StructField("masked_user_id", StringType(), True),
                    StructField("start_date", TimestampType(), True),
                    StructField("end_date", TimestampType(), True),
                    StructField("flag_active", StringType(), True),
                ]
            )
            emptyRDD = spark.sparkContext.emptyRDD()
            lookup_df = spark.createDataFrame(emptyRDD, lookup_schema)
            # write the lookup table
            lookup_df.write.format("delta").mode("overwrite").save(lookup_path)
            delta_table = DeltaTable.forPath(spark, lookup_path)

        # perform scd2 implementation
        targetDF = spark.read.format("delta").load(lookup_path)

        # print(targetDF)
        joinDf = src_df.join(
            targetDF,
            (src_df[piicolumns[0]] == targetDF[piicolumns[0]]) & (targetDF["flag_active"] == "Y"),
            "leftouter",
        ).select(
            src_df["*"],
            targetDF[piicolumns[0]].alias("target_" + piicolumns[0]),
            targetDF[piicolumns[1]].alias("target_" + piicolumns[1]),
            targetDF[piicolumns[2]].alias("target_" + piicolumns[2]),
            targetDF[piicolumns[3]].alias("target_" + piicolumns[3]),
        )

        # print(joinDf)
        filterDF = joinDf.filter(
            xxhash64(joinDf[piicolumns[1]], joinDf[piicolumns[2]], joinDf[piicolumns[3]])
            != xxhash64(
                joinDf["target_" + piicolumns[1]],
                joinDf["target_" + piicolumns[2]],
                joinDf["target_" + piicolumns[3]],
            )
        )
        mergeDf = filterDF.withColumn("MERGEKEY", concat(filterDF[piicolumns[0]]))
        dummyDf = filterDF.filter("target_" + piicolumns[0] + " is not null").withColumn(
            "MERGEKEY", lit(None)
        )
        scdDf = mergeDf.union(dummyDf)
        # print(scdDf)
        delta_table.alias("target").merge(
            source=scdDf,
            condition="concat(target." + piicolumns[0] + ")= MERGEKEY and target.flag_active='Y'",
        ).whenMatchedUpdate(
            set={"flag_active": "'N'", "end_date": "current_date"}
        ).whenNotMatchedInsert(
            values={
                piicolumns[0]: piicolumns[0],
                piicolumns[1]: piicolumns[1],
                piicolumns[2]: piicolumns[2],
                piicolumns[3]: piicolumns[3],
                "flag_active": "'Y'",
                "start_date": "current_date",
                "end_date": "'9999-12-31'",
            }
        ).execute()
    else:
        piicolumns = ["advertising_id", "masked_advertising_id"]
        src_df = src_df.select([col for col in piicolumns])
        try:
            # to get the lookup table
            delta_table = DeltaTable.forPath(spark, lookup_path)
        except:
            lookup_schema = StructType(
                [
                    StructField("advertising_id", StringType(), True),
                    StructField("masked_advertising_id", StringType(), True),
                    StructField("start_date", TimestampType(), True),
                    StructField("end_date", TimestampType(), True),
                    StructField("flag_active", StringType(), True),
                ]
            )
            emptyRDD = spark.sparkContext.emptyRDD()
            lookup_df = spark.createDataFrame(emptyRDD, lookup_schema)
            # write the lookup table
            lookup_df.write.format("delta").mode("overwrite").save(lookup_path)
            delta_table = DeltaTable.forPath(spark, lookup_path)

        # perform scd2 implementation
        targetDF = spark.read.format("delta").load(lookup_path)

        # print(targetDF)
        joinDf = src_df.join(
            targetDF,
            (src_df[piicolumns[0]] == targetDF[piicolumns[0]]) & (targetDF["flag_active"] == "Y"),
            "leftouter",
        ).select(
            src_df["*"],
            targetDF[piicolumns[0]].alias("target_" + piicolumns[0]),
            targetDF[piicolumns[1]].alias("target_" + piicolumns[1]),
        )

        # print(joinDf)
        filterDF = joinDf.filter(
            xxhash64(joinDf[piicolumns[1]]) != xxhash64(joinDf["target_" + piicolumns[1]])
        )
        mergeDf = filterDF.withColumn("MERGEKEY", concat(filterDF[piicolumns[0]]))
        dummyDf = filterDF.filter("target_" + piicolumns[0] + " is not null").withColumn(
            "MERGEKEY", lit(None)
        )
        scdDf = mergeDf.union(dummyDf)
        # print(scdDf)
        delta_table.alias("target").merge(
            source=scdDf,
            condition="concat(target." + piicolumns[0] + ")= MERGEKEY and target.flag_active='Y'",
        ).whenMatchedUpdate(
            set={"flag_active": "'N'", "end_date": "current_date"}
        ).whenNotMatchedInsert(
            values={
                piicolumns[0]: piicolumns[0],
                piicolumns[1]: piicolumns[1],
                "flag_active": "'Y'",
                "start_date": "current_date",
                "end_date": "'9999-12-31'",
            }
        ).execute()

    # store result in lookup table
    print("lookup created")


# In[ ]:


app_config_path = "s3://taishik-landingzone-batch08/Configuration/app_config.json"
app_config = spark.read.json(app_config_path, multiLine=True)
app_config.printSchema()


# In[ ]:


# loading path
raw_path = app_config.collect()[0]["phase-2_transformation"]["source"]["bucket_path"]
staging_path = app_config.collect()[0]["phase-2_transformation"]["destination"]["bucket_path"]

# get dataset
datasets = app_config.collect()[0]["datasets"]


# In[ ]:


def encrypt_value(id):
    sha_value = hashlib.sha256(id.encode()).hexdigest()
    return sha_value


# In[ ]:


def casting_columns(df):
    cast_col = app_config.collect()[0][2]["casting_columns"].asDict()
    for key, value in cast_col.items():
        if value == "StringType":
            if key in df.schema.fieldNames():
                df = df.withColumn(key, concat_ws(",", col(key)))
        else:
            if key in df.schema.fieldNames():
                df = df.withColumn(key, col(key).cast(DecimalType(12, 7)))
    return df


# In[ ]:


def masking_columns(df):
    masking_cols = app_config.collect()[0][2]["masking_columns"]
    for col in masking_cols:
        if col in df.schema.fieldNames():
            df = df.withColumn("masked_" + col, spark_udf(col))
    return df


# In[ ]:


spark_udf = udf(encrypt_value, StringType())

for dataset in datasets:
    try:
        df = spark.read.parquet(str(raw_path) + str(dataset) + ".parquet")
        df = casting_columns(df)
        df = masking_columns(df)
        create_lookup_table(
            "s3://taishik-stagingbucket-batch08/Lookup_Data/" + dataset + "/", df, dataset
        )

        # write data to staging zone after masking and casting
        partition_columns = app_config.collect()[0][2]["partition_columns"]
        df.write.option("header", True).partitionBy(partition_columns).mode("overwrite").parquet(
            str(staging_path) + str(dataset) + ".parquet"
        )

    except:
        continue
