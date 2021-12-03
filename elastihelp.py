from elasticsearch import Elasticsearch
from elasticsearch_dsl import *
import pandas as pd
from string import Template
from flatten_json import flatten
import json
import re




def connect_es(cid, creds):
  
  try:
      es = Elasticsearch(cloud_id=cid,http_auth=(creds))
  except :
      print ("Could not connect to Elasticsearch.")
  return(es)

def invoke_search(es, query_filter, global_vars):
  
  final_filter = ""

  if (len(query_filter) > 0):
    filter_query = r'{"match_phrase": {"$field": "$value"}},'
    filter_template = Template(filter_query)
    for fields in query_filter:
        values = query_filter[fields]
        ind_filter = filter_template.substitute(field = fields, value = values)
        final_filter = final_filter + " " + ind_filter
    final_filter = final_filter[:-1]
    es_query = '{"size": 200,"query":{"bool":{"must":[],"filter":[{"match_all":{}},$filter_query,{"range":{"@timestamp":{"gte": "$st","lte": "$et","format":"strict_date_optional_time"}}}],"should": [],"must_not":[]}}}'
  else:
    es_query = '{"size": 200,"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"range":{"@timestamp":{"gte": "$st","lte": "$et","format":"strict_date_optional_time"}}}],"should": [],"must_not":[]}}}'

  es_query_template = Template(es_query)
  new_query = es_query_template.substitute(filter_query = final_filter, st = global_vars['start_time'], et = global_vars['end_time'])
  results = es.search(index = global_vars['idx_pattern'], body=new_query)
  results_dict = results['hits']['hits']
  results_df = build_df(results_dict)
  return (results_df)
  
def find_uniq(df,field):
    uniq_fields = []
    for i in df[field].unique():
        uniq_fields.append(i)
    return(uniq_fields)
    
def find_events(df,field,value):
    temp = df.loc[df[field] == value]
    return (temp)

def remove_metadata (df):
    for column in df.columns:
        if (column.startswith('agent')):
            df.drop(column,axis=1,inplace=True)
        elif (column.startswith('cloud')):
            df.drop(column,axis=1,inplace=True)
        elif (column.startswith('@timestamp')):
            df.drop(column,axis=1,inplace=True)
        elif (column.startswith('source.geo')):
            df.drop(column,axis=1,inplace=True)
        elif (column.startswith('source.as')):
            df.drop(column,axis=1,inplace=True)

def clean_dataframe (final_data,drop_items):
    drop_items = list(drop_items)
    final_data.drop(drop_items, axis=1, inplace=True)

def first_search(es, query_filter, global_vars):
  first_fields = ['event.module', 'event.dataset', 'event.provider']
  (first_df, first_dict) = sample_aggrigations(first_fields,es,query_filter, global_vars)
  first_info = [first_df, first_dict]
  return(first_info)
  
    
def sample_aggrigations(df, es, query_filter,global_vars):
    aggs = r'"$field": {"terms": {"field": "$field","order": {"_count": "desc"},"size": 20}},'
    aggs_template = Template(aggs)
    tmp_name2 = ""
    final_aggs = ""
    final_filter = ""
    
    if isinstance(df, pd.DataFrame):
        for column in df.columns:
          ind_aggs = aggs_template.substitute(field= column)
          final_aggs = final_aggs +  " " + ind_aggs
    elif isinstance(df, list):
        for column in df:
          ind_aggs = aggs_template.substitute(field= column)
          final_aggs = final_aggs +  " " + ind_aggs
        
    final_aggs = final_aggs[:-1]
    
    if (len(query_filter) == 0):
        aggs_query = '{"size": 0, "aggs": { $total_aggs }, "query": { "bool": { "must": [], "filter": [ { "match_all": {}}, {"range":{"@timestamp":{"gte":"$st","lte":"$et","format":"strict_date_optional_time"}}}], "should": [], "must_not": []}}}'
        aggs_query_template = Template(aggs_query)
        new_query = aggs_query_template.substitute(total_aggs = final_aggs, st = global_vars['start_time'], et = global_vars['end_time'])
    else:
        filter_query = r'{"match_phrase": {"$field": "$value"}},'
        filter_template = Template(filter_query)
        for fields in query_filter:
            values = query_filter[fields]
            ind_filter = filter_template.substitute(field = fields, value = values)
            final_filter = final_filter + " " + ind_filter
        final_filter = final_filter[:-1]
        aggs_query = '{"size": 0, "aggs": { $total_aggs }, "query": { "bool": { "must": [], "filter": [ { "match_all": {} },$filter_query, { "range": {"@timestamp":{"gte": "$st","lte": "$et","format":"strict_date_optional_time"}}}], "should": [], "must_not": [] } }}'
        aggs_query_template = Template(aggs_query)
        new_query = aggs_query_template.substitute(total_aggs = final_aggs, filter_query = final_filter, st = global_vars['start_time'], et = global_vars['end_time'])
  
    
    results = es.search(index=global_vars['idx_pattern'],body=new_query, request_timeout=300)
    
    agg_results = (results['aggregations'])
    all_aggs_dict = {}
    aggs_df = pd.DataFrame()
    for fields in agg_results:
        all_values_list = (agg_results[fields]['buckets'])
        values_dict = {}
        values_list = []
        if ((len(agg_results[fields]['buckets'])) == 0):
            continue
        for field_docCount in all_values_list:
            if isinstance(field_docCount, dict):
                value = field_docCount['key']
                doc_count = field_docCount['doc_count']
                values_list.append(value)
                values_dict[value] = doc_count
            else:
                continue
        number_values = (len(values_list))
        if number_values < 20:
            start = 1
            pad_size = 20 - number_values
            while (start <= pad_size):
                values_list.append("NaN")
                start += 1
        
        all_aggs_dict[fields] = values_dict
        aggs_df[fields] = values_list
    aggs_info = [aggs_df, all_aggs_dict]
    return(aggs_info)

def write_doc(final_data, filename):
    out_file = final_data.to_html()
    output_file = filename + ".html"
    output = open(output_file, "w", encoding="utf-8")
    output.write(out_file)
    output.close()

def build_df(data):
    event_dict = {}
    uniq_fields = {}
    start = 0
    num_events = len(data)
    while start < num_events:
        event = flatten(data[start]['_source'],'.')
        event_dict[start] = event
        for fields in event:
            if ((isinstance(event[fields], list)) or (isinstance(event[fields], list))):
                continue
            if fields not in uniq_fields.keys():
                uniq_fields[fields] = 1
        start += 1
        
    field_names = uniq_fields.keys()
    final_df =  pd.DataFrame(columns=field_names)
    start = 0
    while start < num_events:
        event = event_dict[start]
        df = pd.json_normalize(event)
        final_df = final_df.append(df,ignore_index=True)
        start += 1  
      
    return (final_df)
    
def winvestigate(es,filename,global_vars):
  ecs = open("event.info", 'r')
  lines = ecs.readlines()
  
  columns_list = ['eventID', 'eventCategory', 'eventType','eventAction']
  event_id_df = pd.DataFrame(columns=columns_list)
  start_num = 0
  eventIDs_num = len(lines)
  
  for i in lines:
      (eventID, ecsMappings) = i.split(':', 1)
      eventID = re.sub('"', '', eventID)
      ecsMappings = re.sub('(\[|,$)|(, (//.+))?', '' ,ecsMappings)
      end_eventCategory =  (ecsMappings.find("]"))
      end_eventType = (ecsMappings.find("]", end_eventCategory+1))
  
      eventCategory = (ecsMappings[:end_eventCategory])
      eventType = (ecsMappings[end_eventCategory+2:end_eventType])
      eventAction = (ecsMappings[end_eventType+2:(len(ecsMappings)-2)])
  
      eventCategory = re.sub('\"', '', eventCategory)
      eventType = re.sub('\"', '', eventType)
      eventAction = re.sub('\"', '', eventAction)
  
      eventCategory_list = eventCategory.split(",")
      eventType_list = eventType.split(",")
      eventAction_list = eventAction.split(",")
      
      while (start_num <= eventIDs_num):
          event_id_df.loc[start_num] = [eventID,eventCategory_list, eventType_list, eventAction_list ]
          start_num += 1
          
  eventids_df = pd.read_csv("EventIDMappings.csv")
  eventids_df['Success'] = 'NA'
  eventids_df['Failure'] = 'NA'
  
  #new_query= '{"size":0,"aggs":{"EventID":{"terms":{"field":"winlog.event_id","order":{"_count":"desc"},"size":1000},"aggs":{"Outcome":{"terms":{"field":"event.outcome","order":{"_count":"desc"},"size":5}}}}},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"match_phrase":{"winlog.channel":"Security"}},{"range":{"@timestamp":{"gte":"now-7d/d","lte":"now/d","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}'
  new_query= '{"size":0,"aggs":{"EventID":{"terms":{"field":"event.code","order":{"_count":"desc"},"size":1000},"aggs":{"Outcome":{"terms":{"field":"event.outcome","order":{"_count":"desc"},"size":5}}}}},"query":{"bool":{"must":[],"filter":[{"match_all":{}},{"match_phrase":{"winlog.channel":"Security"}},{"range":{"@timestamp":{"gte":"' + global_vars['start_time'] + '","lte":"' + global_vars['end_time'] + '","format":"strict_date_optional_time"}}}],"should":[],"must_not":[]}}}'

  wineventIDs = es.search(index=global_vars['idx_pattern'],body=new_query, request_timeout=300)

  data = wineventIDs['aggregations']['EventID']['buckets']
  
  for event in data:
    eventID = event['key']
    eventID = int(eventID)
    eventIDCount = event['doc_count']
    for eventOutcomes in event['Outcome']['buckets']:
        eventOutcome = eventOutcomes['key']
        if (eventOutcome == 'success'):
            #print ("updating success:",eventID)
            eventids_df.loc[eventids_df.EventID == eventID, 'Success'] = 'Yes'
        elif (eventOutcome == 'failure'):
            #print ("updating failure")
            eventids_df.loc[eventids_df.EventID == eventID, 'Failure'] = 'Yes'
        eventOutcomeCount = eventOutcomes['doc_count']
        
  write_doc(eventids_df,filename)

  
def field_limit (df, min, max):
  
  if (min > max):
    tempnum = max
    max = min
    min = tempnum
  
  fields_count = {}  
  for (fields, values) in df.iteritems():
    try:
        unique_fields = len(df[fields].unique())
    except Exception:
        continue
    if ((unique_fields > min) and (unique_fields < max)):
      fields_count[fields] = unique_fields
    
  sort_fields = sorted(fields_count.items(), key=lambda x: x[1], reverse=True)
  return (sort_fields)

def enumerate (df,field_name,parent):

    print (field_name)
    parent['children'] = []
    for field_value in df[0][field_name].unique():
        if not (field_value == 'NaN'):
            ind_dict = {}
            ind_dict['index'] = "filebeat-*"
            ind_dict['name'] = field_value
            ind_dict['field'] = field_name
            query_filter[field] = field_value
            field_value_df = invoke_search(es,query_filter)
            ind_dict['info'] = field_value_df
            parent['children'].append(ind_dict)

def list_index(es,index_type):
    idx_name = []
    idx = es.cat.indices().split('\n')

    for i in idx:
        if len(i) > 0:
          index_name = i.split()[2]
          if (index_type == 'A') or (index_type == 'a'):
            idx_name.append(index_name) 
          elif (index_type == 'S') or (index_type == 's'):
            if (index_name.startswith('.')):
              idx_name.append(index_name)
          elif (index_type == 'D') or (index_type == 'd'):
            if not (index_name.startswith('.')):
              idx_name.append(index_name) 
    return(idx_name)