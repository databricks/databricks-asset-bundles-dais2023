from os import path
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, pandas_udf
import pandas as pd
import requests


@dlt.table
def include_medium_metrics():
    df: DataFrame = dlt.read("input_clean")

    metricsDF = df.groupby("link").applyInPandas(get_metrics, schema="link string, claps double, readingTime double")

    # Join on original data, sort by number of claps descending
    finalDF = metricsDF.join(df, on = "link", how = "right_outer").sort(desc("claps"))
    return metricsDF

# Get Medium page HTML and parse clap count
def get_metrics(input_df: pd.DataFrame) -> pd.DataFrame:
    story_url = input_df['link'][0]
    response = story = requests.get(story_url)
    response.raise_for_status()
    story = response.content.decode("utf-8")
    c = story.split('clapCount":')[1]
    r = story.split('readingTime":')[1]
    clapEndIndex = c.index(",")
    rEndIndex = r.index(",")
    claps = int(c[0:clapEndIndex])
    readingTime = float(r[0:rEndIndex])
    result = pd.DataFrame(data={'link': [story_url], 'claps': [claps], 'readingTime': [readingTime]})
    return result
