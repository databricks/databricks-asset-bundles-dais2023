from os import path
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, pandas_udf
import pandas as pd
import requests

@dlt.table
def medium_metrics():
    df: DataFrame = dlt.read("medium_clean")

    metricsDF = df.groupby("link").applyInPandas(get_metrics, schema="link string, claps double, readingTime double")

    # Join on original data, sort by number of claps descending
    finalDF = metricsDF.join(df, on = "link", how = "right_outer").sort(desc("claps"))
    return finalDF

# Get Medium page HTML and parse clap count and reading time
def get_metrics(input_df: pd.DataFrame) -> pd.DataFrame:
    story_url = input_df['link'][0]
    response = story = requests.get(story_url)
    story = response.content.decode("utf-8")
    try:
        c = story.split('clapCount":')[1]
        r = story.split('readingTime":')[1]
        clapEndIndex = c.index(",")
        rEndIndex = r.index(",")
        claps = int(c[0:clapEndIndex])
        readingTime = float(r[0:rEndIndex])
        result = pd.DataFrame(data={'link': [story_url], 'claps': [claps], 'readingTime': [readingTime]})
    except Exception:
        print("Couldnt access URL, returning 0")
        result = pd.DataFrame(data={'link': [story_url], 'claps': [0], 'readingTime': [0]})
    return result

