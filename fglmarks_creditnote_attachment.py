#!/usr/bin/env python


"""
This program creates a text file with index, message-id and claim type.
Use update_index to update labels on Elastic Search from files generated here.
""" 
print("Start program to fetch mails with keywords for labeling")
print("import libraries")
import sys
sys.path.append("..")
import os
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search, Q
from credentials import creds

uname, pwd = creds.uname_pwd()
hosts = "https://ctesm1.discoverdollar.com:41532"
# hosts = "https://10.1.8.6:9200"
es = Elasticsearch(hosts, timeout=60000, use_ssl=False, verify_certs=False, ssl_show_warn=False, http_auth=(uname, pwd))
indexes = ["cantire-2022-10"]  

all_files = [x for x in os.listdir() if x.endswith('.xlsx') or x.endswith('.csv') or x.endswith('xls')]
print('All excels/csvs in folder: ', all_files)

labeling_file = input('Enter name of excel file: ', )

def get_conditions(excel_file):
    readexcel_file = pd.ExcelFile(excel_file)
    sheet_names = readexcel_file.sheet_names
    print('Sheets in the excel file : ', sheet_names)
    sheet = input('Enter the sheet from list above for claim: ', )    
    df = pd.read_excel(readexcel_file, sheet_name=sheet, dtype=str)
    df.loc[df['support'].isna(), 'support'] = pd.Series([ [] for _ in range(len(df.loc[df['support'].isna()])) ])
    df = df[['keyword',	'tag', 'support', 'label']]
    taglabel = df[['tag', 'label']].drop_duplicates().reset_index(drop=True)
    print(taglabel)
    idx = int(input('Enter index for claim type: '))
    tag = taglabel['tag'][idx]
    label_name = taglabel['label'][idx]
    print('Tag is {} and Label is {}: '.format(tag, label_name))
    df = df.loc[(df['tag']==tag) & (df['label']==label_name)]
    print(df.head())
    conditions= []
    for i in df.iterrows():
        if isinstance(i[1]['support'], list):
            conditions.append({'keyword': i[1]['keyword'].lower(), 'support':i[1]['support']})
        elif isinstance(i[1]['support'], str):
            conditions.append({'keyword': i[1]['keyword'], 'support':i[1]['support'].split(',')})
    return conditions, sheet, tag, label_name

labeling_keywords, sheet_name, tag, label_name = get_conditions(labeling_file)
label_type = tag.lower()
fglmarks = input('Enter domain for fglsports.com or marks.com: ', )


file__ =  open('{}_{}_{}.json'.format(fglmarks.split('@')[0], '_attachment_'+label_type, datetime.now().strftime("%d_%m_%Y_%H_%M")), 'w')
count_file__ =  open('{}_{}_emails_count_{}.txt'.format(fglmarks.split('@')[0], '_attachment_'+label_type, datetime.now().strftime("%d_%m_%Y_%H_%M")), 'w')

def get_all_conditions_query(claim_conditions):
    query_list = []
    for condition in claim_conditions:
        if condition["support"] == ['']:
            condition['support'] = []
        query = {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                { "match_phrase_prefix": { "content": condition["keyword"] } },
                                { "match_phrase_prefix": { "metaData.title": condition["keyword"] } }
                            ], "minimum_should_match":1
                        }
                    }],
                "must_not": [
                    { "match_phrase_prefix": { "fileType" : "email" }}
                ]
            }
        }
        if (len(condition["support"]) > 0):
            for supporting_keyword in condition["support"]:
                if (len(query["bool"]["must"]) >= 1):
                    query['bool'].update(filter=[{
                              "bool": {
                                "should": [
                                  {
                                    "match_phrase_prefix": {
                                      "content": supporting_keyword
                                    }
                                  },
                                  {
                                    "match_phrase_prefix": {
                                      "metaData.title": supporting_keyword
                                    }
                                  }
                                ]
                              }
                            }])
                else:
                    query["bool"]["must"][1]["bool"]["should"].append( { "match_phrase_prefix": { "content": supporting_keyword } } )
                    query["bool"]["must"][1]["bool"]["should"].append( { "match_phrase_prefix": { "metaData.title": supporting_keyword }} )
                query_list.append(Q(query))
        else:
             query_list.append(Q(query))
    return query_list

def fetch_email_by_md5(es_, md5, sourcelist, fglmarks):
    query_2 = {
        "bool": {
            "must": [
                    {"bool": 
                    {
                        "should": [
                            {"match_phrase": {"metaData.to": fglmarks}
                            },
                            {"match_phrase": {"metaData.from": fglmarks}
                            },
                            {"match_phrase": {"metaData.cc": fglmarks}
                            },
                        ], "minimum_should_match": 1
                    }},
                    {"match_phrase_prefix": {"fileType" : "Email"}},
                    {"match_phrase_prefix": {"Attachment.MD5":md5}}                                        
                ]          
        }
    }

    t = Search(using=es_, index='cantire-202*')
    t.query = Q(query_2)
    t = t.source(sourcelist)
    for mail in t.scan():
        if "labels" in mail:
            labels = mail["labels"]
        else:
            labels = []
        labels_ = list(set(labels)) + [label_name]
        mailmid_list.add(mail.meta["id"])
        mailmd5s.add(mail['MD5'])
        try:
            file__.write('{ "update": { "_index": "'+ mail.meta["index"] + '", "_type": "_doc", "_id": "'+ mail.meta["id"] +'"} }\n')
            file__.write('{"doc": { "labels": ' + str(labels_).replace("'", '"') + ', ' + label_type + ': "true"} }\n')                        
            i+=1
            if i % 1000 == 0 : print(i, "Processed")
        except Exception as someError:
            print("Exception ", mail.meta.index, mail.meta.id, labels_)
            print(someError)
    return


        
query_list = get_all_conditions_query(labeling_keywords)
sourcelst = ['MD5', 'metaData', 'fileType', 'Attachment', 'labels']
mailmid_list = set()
mailmd5s = set()
attachment_md5s = set()
attachment_mid = set()
i=0
for index__ in indexes:
    print(index__)
    for query__ in query_list:
        s = Search(using=es, index=index__)
        s.query = query__
        print(s.count())
        i=0
        s = s.source(["MD5", 'filePath', 'meta'])
        if 'filter' in query__._params:
            print("Count for {} Labels in Month {} with \"{}\" \"{}\" are : {}".format(label_type, index__, str(query__._params['must'][0]._params['should']), str(query__._params['filter'][0]._params['should']), s.count()))
            print("Count for {} Labels in Month {} with \"{}\" \"{}\" are: {}".format(label_type, index__, str(query__._params['must'][0]._params['should']), str(query__._params['filter'][0]._params['should']), s.count()), file =count_file__)
        else:
            print("Count for {} Labels in Month {} with \"{}\" are : {}".format(label_type, index__, str(query__._params['must'][0]._params['should']), s.count()))
            print("Count for {} Labels in Month {} with \"{}\" are : {}".format(label_type, index__, str(query__._params['must'][0]._params['should']), s.count(), file =count_file__))
        for x in s.scan():
            if '.pdf' in ''.join(x['filePath']).lower() or '.doc' in ''.join(x['filePath']).lower():
                attachment_mid.add(x.meta["id"])
                attachment_md5s.add(x['MD5'])
                fetch_email_by_md5(es, x['MD5'], sourcelst, fglmarks)
                print(len(attachment_md5s), len(attachment_mid))

print('close files')
file__.close()
count_file__.close()

print("Processing complete")