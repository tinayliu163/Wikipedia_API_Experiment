"""
collecting all fields

page id ✅
page texts ✅
redirects ✅
categories ✅Gives the categories in the parsed wikitext.
links ✅ Gives the internal links in the parsed wikitext.
externallinks ✅Gives the external links in the parsed wikitext.
section ✅ Gives the sections in the parsed wikitext.
properties ✅ Gives various properties defined in the parsed wikitext. We can get id in wikidata.org

"""
from pyspark.sql import SparkSession
from ossspark import SparkDataPath
import pyspark.sql.functions as sparkfn
import pandas as pd
from lxml import html
from pyspark.sql.functions import col, regexp_replace, split
import os
from pyspark.sql.types import *

sd = SparkDataPath()


root = "/srv/spark/shared-data-export/liuy/datasets/video_level_classification/Video_game_extraction/wiki_games_exercise" 
spark = SparkSession.builder\
        .appName("Wiki API Games")\
        .getOrCreate()

def input_games(df):
    video_game = pd.read_csv(df)
    video_game['id'] = [item.replace("http://www.wikidata.org/entity/", "") for item in video_game['id'].tolist()]
    video_game = video_game.drop_duplicates(subset = ['name'])
    return video_game[['id', 'idLabel', 'name']]

video_game = input_games(os.path.join(root, 'video_games.csv'))
video_game_series = input_games(os.path.join(root, 'video_games_series.csv'))
series_mapping = input_games(os.path.join(root, 'video_games_series_mapping.csv'))

final_games = video_game.append(video_game_series).append(series_mapping)
final_games = final_games.drop_duplicates(subset = ['name'])
article_titles = final_games['name'].tolist()

# import random
import requests
import json

URL = "https://en.wikipedia.org/w/api.php"

lst_categories = []
lst_page_id = []
lst_links = []
lst_externallinks = []
lst_properties = []
lst_sections = []
lst_redirects = []
lst_texts = []
lst_page_titles = []

for title in article_titles:
    PARAMS = {
    "action": "parse",
    "page": title,
    "format": "json",
    "redirects": True
    
}   
    json_return = requests.get(url=URL, params=PARAMS)
    json_data = json_return.json()

    if 'parse' in json_data.keys():
        #page_id
        page_id = json_data['parse']['pageid']
    
        #categories
        categories = []
        for item in json_data['parse']['categories']:
            if 'hidden' not in item.keys():
                label = item["*"]
                categories.append(label)  
    
        #links
        links = [item['*'] for item in json_data['parse']['links']]
    
        #external links
        external_links = json_data['parse']['externallinks']
    
        #sections
        sections = []
    
        for item in json_data['parse']['sections']:
            section_name = item['line']
            sections.append(section_name)
        
        lst_categories.append(categories)
        lst_page_id.append(page_id)
        lst_links.append(links)
        lst_externallinks.append(external_links)
        lst_sections.append(sections) 
        
        #property
        #properties = json_data['parse']['properties']
            
        #if len(properties) > 0:
            #lst_properties.append(properties[-1]["*"])
        #else:
            #lst_properties.append(None)
        wikidata_id = final_games[final_games['name'] == title].values[0][0]
        lst_properties.append(wikidata_id)
            
        #redirects
        if len(json_data['parse']['redirects']) > 0:
            lst_redirects.append(json_data['parse']['redirects'][0]['to'])
        else:
            lst_redirects.append(None)
            
        #texts
        raw_html = json_data['parse']['text']['*']
        document = html.document_fromstring(raw_html)
        # redirect pages
        #if len(para) > 0 and para[0].text_content().startswith("Redirect to") is False:
        text = ""    
        for idx in range(len(document.xpath('//p'))):
            text = text + " " + str(document.xpath('//p')[idx].text_content())
            text = text.replace("\n", ".")
            
        lst_texts.append(text)
        lst_page_titles.append(title)
    else:
        lst_categories.append(None)
        lst_page_id.append(None)
        lst_links.append(None)
        lst_externallinks.append(None)
        lst_properties.append(None)
        lst_redirects.append(None)
        lst_sections.append(None)
        lst_texts.append(None)
        lst_page_titles.append(None)

data = {'page_id':lst_page_id,'page_title':lst_page_titles, 'page_text': lst_texts, 
        'category': lst_categories, 'links': lst_links, 'external_links': lst_externallinks, 'sections': lst_sections,
        'redirects_page': lst_redirects, 'wikidata_id': lst_properties}

df = pd.DataFrame(data)
df = df[~df['page_title'].isnull()]
df.to_csv(os.path.join(root, 'API_wikipedia_game.csv'))

"""

mySchema = StructType([StructField("page_title", StringType(), True)\
                        ,StructField("page_text", StringType(), True)\
                        ,StructField("category", ArrayType(StringType()), True)\
                        ,StructField("links", ArrayType(StringType()), True)\
                        ,StructField("external_links", ArrayType(StringType()), True)\
                        ,StructField("sections", ArrayType(StringType()), True)\
                        ,StructField("wikidata_id", StringType(), True)])

# spark_df = spark.createDataFrame(games, schema=mySchema)
# spark_df.printSchema()

"""


df = spark.read.format("csv").option("escape", "\"").option("header", True).load(os.path.join(root, 'API_wikipedia_game.csv'))

df = df.withColumn("category",split(regexp_replace(col("category"), r"(^\[)|(\]$)|(')", ""), ", ")) \
        .withColumn("links",split(regexp_replace(col("links"), r"(^\[)|(\]$)|(')", ""), ", ")) \
            .withColumn("external_links",split(regexp_replace(col("external_links"), r"(^\[)|(\]$)|(')", ""), ", ")) \
                .withColumn("sections",split(regexp_replace(col("sections"), r"(^\[)|(\]$)|(')", ""), ", "))


df.write.parquet('/srv/spark/shared-data-export/liuy/datasets/video_level_classification/wiki_games_nonredirect/')



