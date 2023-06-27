from os import path
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, pandas_udf
import pandas as pd
import requests

@dlt.table
def medium_metrics():
    df: DataFrame = dlt.read("medium_clean")

    metricsDF = df.groupby("link").applyInPandas(get_metrics, schema="link string, claps double")

    # Join on original data, sort by number of claps descending
    finalDF = metricsDF.join(df, on = "link", how = "right_outer").sort(desc("claps"))
    return finalDF

# Get Medium page HTML and parse clap count
def get_metrics(input_df: pd.DataFrame) -> pd.DataFrame:
    story_url = input_df['link'][0]
    response = story = requests.get(story_url)
    story = response.content.decode("utf-8")
    try:
        c = story.split('clapCount":')[1]
        clapEndIndex = c.index(",")
        claps = int(c[0:clapEndIndex])
        result = pd.DataFrame(data={'link': [story_url], 'claps': [claps]})
    except Exception:
        print("Couldnt access URL, returning 0")
        result = pd.DataFrame(data={'link': [story_url], 'claps': [0]})
    return result
