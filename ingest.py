from os import path

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace


@dlt.table
@dlt.expect("No null links", "link is not null")
def medium_raw():
    csv_path = "dbfs:/data-asset-bundles-dais2023/fe_medium_posts_raw.csv"
    return spark.read.csv(csv_path, header=True)


@dlt.table
def medium_clean():
    df: DataFrame = dlt.read("medium_raw")
    df = df.filter(df.link != 'null')
    df = df.withColumn("author", regexp_replace("author", "\\([^()]*\\)", ""))
    return df
