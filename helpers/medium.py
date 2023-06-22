import pandas as pd
import requests

# Get Medium page HTML and parse clap count
def get_claps(input_df: pd.DataFrame) -> pd.DataFrame:
    story_url = input_df['link'][0]
    c = requests.get(story_url).content.decode("utf-8")
    c = c.split('clapCount":')[1]
    endIndex = c.index(",")
    claps = int(c[0:endIndex])
    result = pd.DataFrame(data={'link': [story_url], 'claps': [claps]})
    return result