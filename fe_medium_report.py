# Databricks notebook source
# MAGIC %md ## Top Medium Posts by Databricks Field Engineering

# COMMAND ----------

# MAGIC %md ### Read Data

# COMMAND ----------

from pyspark.sql.functions import desc

dbutils.widgets.text("dbname", "")

# COMMAND ----------

# Read Medium metrics table
full_table_path = "hive_metastore." + dbutils.widgets.get("dbname") + ".medium_metrics"
enrichedDF = spark.read.table(full_table_path)

# COMMAND ----------

# MAGIC %md ### Visualize

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 20 Articles by Applause

# COMMAND ----------

# Import necessary libraries
import plotly.express as px

# Get top articles
top_articles = enrichedDF.toPandas().head(20)

# Create bar chart using Top 20 articles data
fig = px.bar(top_articles, x='author', y='claps', 
             labels={'author':'Article Author', 'claps':'Number of Claps'},
             hover_data={'author': True, 'link': True, 'summary': True},
             height=400)

# Update chart layout
fig.update_layout(title_text='Top 20 Articles by Applause', 
                  xaxis_title='Author', 
                  yaxis_title='Claps',
                  plot_bgcolor='white')

# Display chart
displayHTML(fig.to_html(full_html=False))

# COMMAND ----------

# MAGIC %md #### Explore full dataset

# COMMAND ----------

display(enrichedDF)
