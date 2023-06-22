from os import path

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc
import pandas as pd
import requests


@dlt.table
def include_medium_claps():
    df: DataFrame = dlt.read("input_clean")

    clapsDF = df.groupby("link").applyInPandas(get_claps, schema="link string, claps double")

    # Join on original data, sort by number of claps descending
    finalDF = clapsDF.join(df, on = "link", how = "right_outer").sort(desc("claps"))

    return finalDF


# Get Medium page HTML and parse clap count
def get_claps(input_df: pd.DataFrame) -> pd.DataFrame:
    story_url = input_df['link'][0]
    c = requests.get(story_url).content.decode("utf-8")
    c = c.split('clapCount":')[1]
    endIndex = c.index(",")
    claps = int(c[0:endIndex])
    result = pd.DataFrame(data={'link': [story_url], 'claps': [claps]})
    return result
