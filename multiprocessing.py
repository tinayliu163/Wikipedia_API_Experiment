import requests
import json
from lxml import html
from pyspark.sql import Row
import pyspark.sql.functions as sparkfn
from pyspark.sql.types import *
from  multiprocessing import Process,Pool, cpu_count
import os, time, random
import itertools
from functools import reduce 
from pyspark.sql import DataFrame

def crawler(title_list):

    lst_categories = []
    lst_page_id = []
    lst_links = []
    lst_externallinks = []
    lst_sections = []
    lst_redirects = []
    lst_texts = []
    lst_page_titles = []


    URL = "https://en.wikipedia.org/w/api.php"
    for title in title_list:
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

    mySchema = StructType([StructField("page_id", StringType(), True)\
                       ,StructField("page_title", StringType(), True)\
                       ,StructField("page_text", StringType(), True)\
                       ,StructField("category", ArrayType(StringType()), True)\
                       ,StructField("links", ArrayType(StringType()), True)\
                       ,StructField("external_links", ArrayType(StringType()), True)\
                       ,StructField("sections", ArrayType(StringType()), True)\
                       ,StructField("redirects_page", StringType(), True)])

   
    data = [{'page_id':lst_page_id,'page_title':lst_page_titles, 'page_text': lst_texts, 'category': lst_categories, 'links': lst_links, 
             'external_links': lst_externallinks, 'sections': lst_sections, 'redirects_page': lst_redirects} 
            for lst_page_id,lst_page_titles,lst_texts,lst_categories, lst_links, lst_externallinks, lst_sections, lst_redirects
            in zip(lst_page_id,lst_page_titles,lst_texts,lst_categories, lst_links, lst_externallinks, lst_sections, lst_redirects)]

    df = spark.createDataFrame(data, schema = mySchema)
    df = df.where(~sparkfn.array_contains(df.category, 'Disambiguation_pages'))
    df = df.where(~sparkfn.array_contains(df.category, 'Disambiguation pages'))
    return df

with open('wikiListOfArticles_nonredirects.txt') as f:
    content = f.readlines()
    
samp_titles = [x.split('; ')[1].strip() for x in content]
len(samp_titles)

def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def unionAll(dfs):
    return reduce(DataFrame.unionAll, dfs)

if __name__ == '__main__':
    
    num_cores = cpu_count()   
    results = []
    pool = Pool(num_cores)
    
    chunks = list(split(samp_titles, 100))
    stime = time.time()
    print(time.time()-stime)
    result = pool.map(crawler,chunks) 
    print(time.time()-stime) 
    results.extend(result)
    pool.close()
    pool.join()
    print(time.time()-stime)
    
