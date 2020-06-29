from pyspark.sql import SparkSession
from ossspark import SparkDataPath
import pyspark.sql.functions as sparkfsn
import pandas as pd
from simplemediawiki import MediaWiki
from lxml import html
from pyspark.sql.functions import col, regexp_replace, split
import os
from pyspark.sql.types import *
import time
import random

sd = SparkDataPath()


root = "/srv/spark/shared-data-export/liuy/datasets/video_level_classification/Video_game_extraction/wiki_games_exercise" 
spark = SparkSession.builder\
        .appName("Wiki API Nongame")\
        .getOrCreate()


listOfPagesFile = open("wikiListOfArticles_nonredirects.txt", "w")
wiki = MediaWiki('https://en.wikipedia.org/w/api.php')

continueParam = ''
requestObj = {}
requestObj['action'] = 'query'
requestObj['list'] = 'allpages'
requestObj['aplimit'] = 'max'
requestObj['apnamespace'] = '0'
requestObj['apfilterredir'] = 'nonredirects'
requestObj['apfilterredir'] = 'nonredirects'
requestObj

pagelist = wiki.call(requestObj)
pagesInQuery = pagelist['query']['allpages']

for eachPage in pagesInQuery:
    pageId = eachPage['pageid']
    title = eachPage['title']
    writestr = str(pageId) + "; " + title + "\n"
    listOfPagesFile.write(writestr)

stime = time.time()
numQueries = 1
try:
    while len(pagelist["continue"]) > 0:

        requestObj['apcontinue'] = pagelist["continue"]["apcontinue"]
        pagelist = wiki.call(requestObj)

        pagesInQuery = pagelist['query']['allpages']
    
        for eachPage in pagesInQuery:
            pageId = eachPage['pageid']
            title = eachPage['title']
            writestr = str(pageId) + "; " + title + "\n"
            listOfPagesFile.write(writestr)
            # print writestr

        numQueries += 1
        if numQueries % 1000 == 0:
            print("Done with queries -- ", numQueries)
            print(numQueries)
except:
    print('done')

listOfPagesFile.close()

print(time.time()-stime)

"""
Totally gather 6,071,005 non-redirects

"""

with open('wikiListOfArticles_nonredirects.txt') as f:
    content = f.readlines()
    
titles = [x.split('; ')[1].strip() for x in content]
len(titles)

"""
Exclude game pages
"""

wiki_game = spark.read.csv(os.path.join(root, 'API_wikipedia_game.csv'), header = True)
wiki_game = wiki_game.dropDuplicates(subset = ['page_title'])
game_title = [row.page_title for row in wiki_game.select('page_title').collect()]
non_game_title = [item for item in titles if item not in game_title]


sample_size = len(game_title)
samp_non_game_title = random.sample(non_game_title, sample_size)
len(samp_non_game_title)

# import random
import requests
import json

lst_categories = []
lst_page_id = []
lst_links = []
lst_externallinks = []
lst_sections = []
lst_redirects = []
lst_texts = []
lst_page_titles = []


URL = "https://en.wikipedia.org/w/api.php"
for title in samp_non_game_title:
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
        
        lst_page_id.append(page_id)
        lst_categories.append(categories)
        lst_links.append(links)
        lst_externallinks.append(external_links)
        lst_sections.append(sections) 
            
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
        lst_page_id.append(None)
        lst_categories.append(None)
        lst_links.append(None)
        lst_externallinks.append(None)
        lst_sections.append(None)
        lst_redirects.append(None)
        lst_texts.append(None)
        lst_page_titles.append(None)

data = {'page_id':lst_page_id,'page_title':lst_page_titles, 'page_text': lst_texts, 
        'category': lst_categories, 'links': lst_links, 'external_links': lst_externallinks, 'sections': lst_sections,
        'redirects_page': lst_redirects}

df = pd.DataFrame(data)
df_sample = df[~df['page_title'].isnull()]

redirect_pages = df_sample[~df_sample['redirects_page'].isnull()]
redirect_pages['page_title'] = [item for item in redirect_pages['redirects_page'].tolist()]
target_page = df_sample[df_sample['redirects_page'].isnull()]
non_games = target_page.append(redirect_pages)
non_games = non_games.drop(columns = ['page_id', 'redirects_page'])


non_games.to_csv(os.path.join(root, 'API_wikipedia_nongame_samp.csv'))

API_non_game = spark.read.format("csv").option("escape", "\"").option("header", True).load("API_wikipedia_nongame_samp.csv").drop('_c0') 
from pyspark.sql.functions import col, regexp_replace, split

API_non_game = API_non_game.withColumn("category",split(regexp_replace(col("category"), r"(^\[)|(\]$)|(')", ""), ", ")) \
        .withColumn("links",split(regexp_replace(col("links"), r"(^\[)|(\]$)|(')", ""), ", ")) \
            .withColumn("external_links",split(regexp_replace(col("external_links"), r"(^\[)|(\]$)|(')", ""), ", ")) \
                .withColumn("sections",split(regexp_replace(col("sections"), r"(^\[)|(\]$)|(')", ""), ", "))

API_non_game.write.parquet(os.path.join(root, '/srv/spark/shared-data-export/liuy/datasets/video_level_classification/wiki_nongames_samp'))


