#!/usr/bin/env python


"""
This program creates a csv file with index, message-id and claim type.
Use update_index to update labels on Elastic Search from files generated here.
""" 
print("Start program to fetch mails with keywords for labeling")
print("import libraries")
import os
import sys
sys.path.append("..")
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search, Q
from credentials import usr_pwd

uname, pwd = usr_pwd()

hosts = ["10.1.8.6:9200"]
# hosts = "https://ctesm1.discoverdollar.com:41532"
# hosts = "https://10.1.8.6:9200"
es = Elasticsearch(hosts, timeout=60000, use_ssl=True, verify_certs=False, ssl_show_warn=False, http_auth=(uname, pwd))

indexes = ["cantire-2021-01","cantire-2021-02","cantire-2021-03","cantire-2021-04","cantire-2021-05", "cantire-2021-06",
"cantire-2021-07","cantire-2021-08","cantire-2021-09","cantire-2021-10","cantire-2021-11","cantire-2021-12","cantire-2022-01",
"cantire-2022-02","cantire-2022-03","cantire-2022-04","cantire-2022-05","cantire-2022-06","cantire-2022-07","cantire-2022-08",
"cantire-2022-09","cantire-2022-10","cantire-2022-11"]   
           
all_files = [x for x in os.listdir() if x.endswith('.xlsx') or x.endswith('.csv') or x.endswith('xls')]
print('All excels/csvs in folder: ', all_files)

labeling_file = input('Enter name of excel file: ', )


class ES_labeling:
    def __init__(self, excel_file):
        self.excel_file = excel_file

    def __get_conditions__(self):
        readexcel_file = pd.ExcelFile(self.excel_file)
        sheet_names = readexcel_file.sheet_names
        print('Sheets in the excel file:', sheet_names)
        sheet = input('Enter the sheet from list above for claim: ', )
        negative_list = pd.read_excel(readexcel_file, sheet_name = 'NegativeDomains', header=None)
        self.negative_list = negative_list[0].values.tolist()    
        df = pd.read_excel(readexcel_file, sheet_name=sheet, dtype=str)
        df.loc[df['support'].isna(), 'support'] = pd.Series([ [] for _ in range(len(df.loc[df['support'].isna()])) ])    
        df = df[['keyword',	'tag', 'support', 'label']]
        taglabel = df[['tag', 'label']].drop_duplicates().reset_index(drop=True)
        print(taglabel)
        idx = int(input('Enter number for claim type: ', ))
        tag = taglabel['tag'][idx]
        self.label_name = taglabel['label'][idx]
        print('Tag is {} and Label is {}: '.format(tag, self.label_name))
        df = df.loc[(df['tag']==tag) & (df['label']==self.label_name)]
        print(df.head())   
        self.tag = tag.lower() 
        self.conditions= []
        for i in df.iterrows():
            if isinstance(i[1]['support'], list):
                self.conditions.append({'keyword': i[1]['keyword'], 'support':i[1]['support']})
            elif isinstance(i[1]['support'], str):
                self.conditions.append({'keyword': i[1]['keyword'], 'support':i[1]['support'].split(',')})
        return self.conditions, sheet, self.negative_list, self.tag, self.label_name

    def __get_mail_conditions_query__(self, conditions, label_name):
        self.query_list = []
        for condition in conditions:
            if condition["support"] == ['']:
                condition['support'] = []
            query = {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    { "match_phrase_prefix": { "content": condition["keyword"] } },

                                    { "match_phrase_prefix": { "metaData.subject": condition["keyword"] } }
                                ]
                            }
                        },
                        { "match_phrase_prefix": { "fileType" : "Email" }}
                    ],
                    "must_not" : [{"match_phrase_prefix": {"labels":label_name}}]
                }
            }
            if (len(condition["support"]) > 0):
                for supporting_keyword in condition["support"]:
                    print(supporting_keyword)
                    if (len(query["bool"]["must"]) >= 2):
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
                                        "metaData.subject": supporting_keyword
                                        }
                                    }
                                    ]
                                }
                                }])
                    else:
                        query["bool"]["must"][2]["bool"]["should"].append( { "match_phrase_prefix": { "content": supporting_keyword } } )
                        query["bool"]["must"][2]["bool"]["should"].append( { "match_phrase_prefix": { "metaData.subject": supporting_keyword }} )
                    self.query_list.append(Q(query))
            else:
                self.query_list.append(Q(query))
        return self.query_list

    def __get_attachment_conditions_query__(self, claim_conditions):
        self.query_list = []
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
                    self.query_list.append(Q(query))
            else:
                self.query_list.append(Q(query))
        return self.query_list


ES_labs = ES_labeling(labeling_file)
labeling_keywords, sheet_name, negative, label_type, label_name = ES_labs.__get_conditions__()
query_list = ES_labs.__get_mail_conditions_query__(labeling_keywords, label_name)


file__ =  open('{}_{}_{}.json'.format(label_type, 'mails', datetime.now().strftime("%d_%m_%Y_%H_%M")), 'w')
count_file__ =  open('{}_{}_count_{}.txt'.format(label_type, 'mails', datetime.now().strftime("%d_%m_%Y_%H_%M")), 'w')
mid_list = []
md5s = set()
i=0
for index__ in indexes:
    print(index__)
    for query__ in query_list:
        s = Search(using=es, index=index__)
        s.query = query__
        print(s.count())
        s = s.source(['labels',"MD5", "Attachment", "metaData"])
        if 'filter' in query__._params:
            print("Count for {} Labels in Month {} with \"{}\" \"{}\" are : {}".format(label_type, index__, str(query__._params['must'][0]._params['should']), str(query__._params['filter'][0]._params['should']), s.count()))
            print("Count for {} Labels in Month {} with \"{}\" \"{}\" are: {}".format(label_type, index__, str(query__._params['must'][0]._params['should']), str(query__._params['filter'][0]._params['should']), s.count()), file =count_file__)
        else:
            print("Count for {} Labels in Month {} with \"{}\" are : {}".format(label_type, index__, str(query__._params['must'][0]._params['should']), s.count()))
            print("Count for {} Labels in Month {} with \"{}\" are : {}".format(label_type, index__, str(query__._params['must'][0]._params['should']), s.count(), file =count_file__))
        for x in s.scan():
            reject = False
            if "from" in x.metaData:
                for xval in x.metaData["from"]:
                    try:
                        if any([val for val in negative if val in xval.lower()]):
                            reject = True
                            break
                    except:
                        pass
            if "to" in x.metaData:
                for xval in x.metaData["to"]:
                    try:
                        if any([val for val in negative if val in xval.lower()]):
                            reject = True
                            break
                    except:
                        pass
            if "cc" in x.metaData:
                for xval in x.metaData["cc"]:
                    try:
                        if any([val for val in negative if val in xval.lower()]):
                            reject = True
                            break
                    except:
                        pass
            if reject ==False:
                if "labels" in x:
                    labels = x["labels"]
                else:
                    labels = []
                labels_ = list(set(labels)) + [label_name]
                mid_list.append(x.meta["id"])
                try:
                    file__.write('{ "update": { "_index": "'+ x.meta["index"] + '", "_type": "_doc", "_id": "'+ x.meta["id"] +'"} }\n')
                    file__.write('{"doc": { "labels": ' + str(labels_).replace("'", '"') + ', ' + label_type + ': "true"} }\n')
                    md5s.add(x['MD5'])
                    i+=1
                    if i % 1000 == 0 : print(i, "Processed")
                except Exception as someError:
                    print("Exception ", x.meta.index, x.meta.id, labels_)
                    print(someError)
        print(len(md5s), len(mid_list))

file__.close()
count_file__.close()

print("Processing complete for mails")

print("Now processing for attachments")


def fetch_email_by_md5(es_, md5, sourcelist):
    query_2 = {
        "bool": {
            "must": [
                    {"match_phrase_prefix": {"fileType" : "Email"}},
                    {"match_phrase_prefix": {"Attachment.MD5":md5}}                                        
                ]          
        }
    }
    t = Search(using=es_, index=["cantire-2021-01","cantire-2021-02","cantire-2021-03","cantire-2021-04","cantire-2021-05", "cantire-2021-06",
              "cantire-2021-07","cantire-2021-08","cantire-2021-09","cantire-2021-10","cantire-2021-11","cantire-2021-12","cantire-2022-01",
              "cantire-2022-02","cantire-2022-03","cantire-2022-04","cantire-2022-05","cantire-2022-06","cantire-2022-07","cantire-2022-08",
              "cantire-2022-09","cantire-2022-10","cantire-2022-11"])
    t.query = Q(query_2)
    t = t.source(sourcelist)
    for mail in t.scan():
        reject = False
        if "from" in mail.metaData:
            for xval in mail.metaData["from"]:
                try:
                    if any([val for val in negative if val in xval.lower()]):
                        reject = True
                        break
                except:
                    pass
        if "to" in mail.metaData:
            for xval in mail.metaData["to"]:
                try:
                    if any([val for val in negative if val in xval.lower()]):
                        reject = True
                        break
                except:
                    pass
        if "cc" in mail.metaData:
            for xval in mail.metaData["cc"]:
                try:
                    if any([val for val in negative if val in xval.lower()]):
                        reject = True
                        break
                except:
                    pass
        if reject ==False:
            if "labels" in mail:
                labels = mail["labels"]
            else:
                labels = []
            labels_ = list(set(labels)) + [label_name]
            mailmid_list.add(mail.meta["id"])
            try:
                file__.write('{ "update": { "_index": "'+ mail.meta["index"] + '", "_type": "_doc", "_id": "'+ mail.meta["id"] +'"} }\n')
                file__.write('{"doc": { "labels": ' + str(labels_).replace("'", '"') + ', ' + label_type + ': "true"} }\n')
                mailmd5s.add(mail['MD5'])
                global j
                j+=1
                if j % 1000 == 0 : print(j, "Processed")
            except Exception as someError:
                print("Exception ", mail.meta.index, mail.meta.id, labels_)
                print(someError)
    print(len(mailmd5s), len(mailmid_list))

    return
        
attachquery_list = ES_labs.__get_attachment_conditions_query__(labeling_keywords)

mailmid_list = set()
mailmd5s = set()
attachment_md5s = set()
attachment_mid = set()
j=0

file__ =  open('{}_{}_{}.json'.format(label_type, 'attachment', datetime.now().strftime("%d_%m_%Y_%H_%M")), 'w')
count_file__ =  open('{}_{}_count_{}.txt'.format(label_type, 'attachment', datetime.now().strftime("%d_%m_%Y_%H_%M")), 'w')

sourcelst = ['MD5', 'metaData', 'fileType', 'Attachment', 'labels']

for index__ in indexes:
    print(index__)
    for query__ in attachquery_list:
        s = Search(using=es, index=index__)
        s.query = query__
        print(s.count())
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
                fetch_email_by_md5(es, x['MD5'], sourcelst)
                print(len(attachment_md5s), len(attachment_mid))

print('close files')
file__.close()
count_file__.close()

print("Processing complete")
