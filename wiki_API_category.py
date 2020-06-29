from pyspark.sql import SparkSession
from ossspark import SparkDataPath
import pyspark.sql.functions as sparkfn
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

"""
gather categories using list:Allcategories

"""
listOfCatFile = open("wikiListOfArticles_categories.txt", "w")
wiki = MediaWiki('https://en.wikipedia.org/w/api.php')

continueParam = ''
requestObj = {}
requestObj['action'] = 'query'
requestObj['list'] = 'allcategories'
requestObj['aclimit'] = 'max'
requestObj['acprop'] = 'size'
print(requestObj)

Catlist = wiki.call(requestObj)
CatInQuery = Catlist['query']['allcategories']

for eachCat in CatInQuery:
    CatSize = eachCat['size']
    CatPage = eachCat['pages']
    CatFile = eachCat['files']
    Subcat = eachCat['subcats']
    Cattitle = eachCat['*']
    writestr = str(CatSize) + "; " + str(CatPage) + "; " + str(CatFile) + "; " + str(Subcat) + "; " + Cattitle + "\n"
    listOfCatFile.write(writestr)

numQueries = 1
try:
    while len(Catlist["continue"]) > 0:

        requestObj['accontinue'] = Catlist["continue"]["accontinue"]
        Catlist = wiki.call(requestObj)

        CatInQuery = Catlist['query']['allcategories']
    
        for eachCat in CatInQuery:
            CatSize = eachCat['size']
            CatPage = eachCat['pages']
            CatFile = eachCat['files']
            Subcat = eachCat['subcats']
            Cattitle = eachCat['*']
            writestr = str(CatSize) + ";" + str(CatPage) + ";" + str(CatFile) + ";" + str(Subcat) + ";" + Cattitle + "\n"
            listOfCatFile.write(writestr)
        # print writestr

        numQueries += 1
        if numQueries % 1000 == 0:
            print("Done with queries -- ", numQueries)
            print(numQueries)
except:
    print('Done')
    
listOfCatFile.close()

with open('wikiListOfArticles_categories.txt') as f:
    content = f.readlines()
print(len(content))

CatSize = [int(item.split(';')[0]) for item in content]
CatPage = [int(item.split(';')[1]) for item in content]
CatFile = [int(item.split(';')[2]) for item in content]
Subcat = [int(item.split(';')[3]) for item in content]
Cattitle = [item.split(';')[4] for item in content]
Cattitle = [item.split('\n')[0].strip() for item in Cattitle]

from pyspark.sql.types import *
mySchema = StructType([StructField("category", StringType(), True)\
                       ,StructField("size", IntegerType(), True)\
                       ,StructField("num_pages", IntegerType(), True)\
                       ,StructField("num_subcat", IntegerType(), True)\
                       ,StructField("num_files", IntegerType(), True)])
   
data = [{'category': Cattitle,'size': CatSize, 'num_pages': CatPage, 'num_subcat': Subcat, 'num_files': CatFile} 
            for Cattitle, CatSize, CatPage, Subcat, CatFile
            in zip(Cattitle, CatSize, CatPage, Subcat, CatFile)]

df = spark.createDataFrame(data, schema = mySchema)

df.write.parquet(os.path.join(root, 'wikipeida_category'))
