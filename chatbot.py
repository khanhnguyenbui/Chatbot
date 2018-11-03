#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 15:06:46 2018

@author: khanhbui
"""

import json
from datetime import datetime
import pandas as pd
import pymongo
from pymongo import MongoClient
import pprint


commentDict = dict()


        
# Mongo Client

client = MongoClient('localhost', 27017)
chatbotdb = client['chatbotdb']
collection = chatbotdb['chatbotcollection']
posts = chatbotdb.posts

#collection.insert_one(row)
#pprint.pprint(collection.find_one())
collection.estimated_document_count()

print(client.list_database_names())

# DROP

# Insert to db
# Read from files RC2006-12: 61018 // 2010-02: 2687779
row_counter = 0
with open("/Users/khanhbui/Desktop/Python/Chatbot/RC_2010-02") as f:
    for row in f:
        row_save = dict()
        row_counter += 1
        row = json.loads(row)
        row_save['index'] = row_counter
        row_save['body'] = row['body']
        #row_save['link_id'] = row['link_id'] #link thread with order
        row_save['parent_id'] = row['parent_id'] #link parent with order
        row_save['id'] = row['id'] #link post
        collection.insert_one(row_save)
        #commentDict[row_counter]=row
        print(row_counter)
        #if row_counter == 100000:
        #   break

collection.estimated_document_count()

#create index
collection.create_index('index')
collection.create_index('parent_id')
collection.create_index('id')
collection.index_information()

for i in range(1,11):
    row = collection.find_one({"index":10011})
    print(i)
    #write from
    with open('/Users/khanhbui/Desktop/Python/Chatbot/test.from','a', encoding='utf8') as f:
        f.write(str(row['body']).replace('\r\n', ' ') + '\n')
    
    #write to
    parent_row = collection.find_one({'link_id':row['parent_id']})
    
    
#WRITE TRAIN TEST FILE
count=0
flags = list()
for i in range(1,collection.estimated_document_count()):
    row = collection.find_one({"index":i})
    parent_id = row['parent_id'].split('_',1)[1]
    #print('row = ' + str(i))
    
    if row['id'] != row['parent_id']:
        #print('parent_id != link_id at '+str(i))
        parent_row = collection.find_one({'id':parent_id})
        if parent_row != None:
            flags.append(i)
            print(count)
            if len(row['body']) <= 1000 and len(parent_row['body']) <=1000 and '[deleted]' not in row['body'] and '[deleted]' not in parent_row['body']:
                count += 1
                #print(str(i) + '\n\n\n')
                #print('child: ' + row['body'] + '\n')
                #print('parrent: ' + parent_row['body'])
                #WRITE FROM TO TRAIN
                if count > 5000:
                    with open('/Users/khanhbui/Desktop/Python/Chatbot/train.to','a', encoding='utf8') as f:
                        f.write(str(row['body']).replace('\n', ' ') + '\n')
                        with open('/Users/khanhbui/Desktop/Python/Chatbot/train.from','a', encoding='utf8') as f:
                            f.write(str(parent_row['body']).replace('\n', ' ') + '\n')
                else:
                #write from-to test
                    with open('/Users/khanhbui/Desktop/Python/Chatbot/test.to','a', encoding='utf8') as f:
                        f.write(str(row['body']).replace('\n', ' ') + '\n')
                    with open('/Users/khanhbui/Desktop/Python/Chatbot/test.from','a', encoding='utf8') as f:
                        f.write(str(parent_row['body']).replace('\n', ' ') + '\n')
    if count == 105000:
       break
    
    

    
    

limit = 5000
last_unix = 0
cur_length = limit
counter = 0
test_done = False
        

df = pd.DataFrame.from_dict(row, orient='index')

last_unix = df.tail(1)['unix'].values[0]

cur_length = len(df)


if not test_done:
    with open('test.from','a', encoding='utf8') as f:
        for content in df['parent'].values:
            f.write(content+'\n')

    with open('test.to','a', encoding='utf8') as f:
        for content in df['comment'].values:
            f.write(str(content)+'\n')

    test_done = True

else:
    with open('train.from','a', encoding='utf8') as f:
        for content in df['parent'].values:
            f.write(content+'\n')

    with open('train.to','a', encoding='utf8') as f:
        for content in df['comment'].values:
            f.write(str(content)+'\n')
    
