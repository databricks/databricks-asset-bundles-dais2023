# Databricks notebook source
# MAGIC %md ### Medium Claps
# MAGIC
# MAGIC #### Main report
# MAGIC 1. Start with csv of URLs, author, published_on
# MAGIC 2. Load csv with DLT, add claps in DLT pipeline, create final output table for visualization
# MAGIC 3. Visualize results
# MAGIC
# MAGIC #### Demo flow
# MAGIC 1. We have a bundle already, our data asset is a DLT pipeline and report that captures claps.  When running in dev, we limit the number of URLs we scrape.  In QA/Prod we run the full pipeline.
# MAGIC 2. Let's add to our data asset - read time and/or followers
# MAGIC 3. We make a change, commit, PR, bundle run fails - code not merged. 
# MAGIC 4. Fix code, re-commit, PR, bundle run successful - code merged.

# COMMAND ----------

# MAGIC %md ### Read CSV

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

mediumRawDF = spark.read.csv('file:/Workspace/Repos/rafi.kurlansik@databricks.com/data-asset-bundles-dais2023/data/fe_medium_posts_raw.csv', header=True)

# Remove null links and clean author column
mediumCleanDF = mediumRawDF.filter(mediumRawDF.link != 'null').withColumn("author", regexp_replace("author", "\\([^()]*\\)", ""))
display(mediumCleanDF)

# COMMAND ----------

# MAGIC %md ### Get Claps
# MAGIC
# MAGIC Source: https://github.com/FrenchTechLead/medium-stats-api

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, desc
from helpers.medium import get_metrics
  
# Apply UDF
enrichedDF = mediumCleanDF.groupby("link").applyInPandas(get_metrics, schema="link string, claps double, readingTime double")

# Join on original data, sort by number of claps descending
finalDF = enrichedDF.join(mediumCleanDF, on = "link", how = "right_outer").sort(desc("claps"))

# COMMAND ----------

# MAGIC %md ### Visualize

# COMMAND ----------

display(finalDF)
