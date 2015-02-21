'''
Created on Feb 19, 2015

@author: alexmol
'''


import sys
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('cp1252')

import csv
from pyspark import SparkContext, SparkConf
from elasticsearch import Elasticsearch
from io import BytesIO

# CONSTANTS
ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = "temptesting"
TYPE_NAME = "rawevent"

es_write_conf = {
     "es.nodes" : "localhost",
     "es.port" : "9200",
     "es.resource" : "temptesting/event"
 } 


"""Define a set of functions that can be used for populating the fields"""
def fixed_value(data, raw_data, value):
    return value
 
def value_by_position(data, raw_data, position):
    return data[position] 

def value_by_regex(data, raw_data, reg_expression):
    # TODO: Implement this
    return ""

def value_by_custom_function(data, raw_data, custom_function):
    # TODO: Implement this
    return ""

# Register all the function names
possibles = globals().copy()
possibles.update(locals())


# List with all the known columns - Temporary
KNOWN_COLUMNS = "EventID", "TimeStamp", "system_messagetype", "Tag", "TagDescription", "Plant", "Area", "Unit", "AlarmIdentifier", "system_all", "system_messagetypename", "system_inputname", "system_spare1", "Priority", "Console", "Operator", "Message", "Parameter", "FromValue", "ToValue", "UnitOfMeasure", "Limit", "Value", "User1", "Suppressed", "User2", "User3", "User4", "User5", "User6", "User7", "User8", "User9", "User10", "system_annotation"    


# TODO: Think about a better way of interacting with this one
mappings = {
            "ALARM":{
                     "Company":("fixed_value", "ST"),
                     "Plant":("fixed_value", "HEIM"),
                     "Area":("fixed_value", "SKR"),
                     "Unit":("fixed_value", ""),
                     "GPS":("fixed_value", ""),
                     "EventID":("value_by_position", 1),
                     "AlarmIdentifier": ("value_by_position", 12),
                     "TimeStamp":("value_by_position", 2),
                     "Tag": ("value_by_position", 9),
                     "TagDescription": ("value_by_position", 10),
                     "Message": ("value_by_position", 11),
                     "User1": ("value_by_position", 6),
                     "User2": ("value_by_position", 7),
                     "User3": ("value_by_position", 8),
                     "User4": ("value_by_position", 5),
                     "User5": ("value_by_position", 9),
                     "User7": ("value_by_position", 4),
                     "User8": ("value_by_position", 13),
                     "User9": ("value_by_position", 1),
                     "User10": ("value_by_position", 3)
                     },
            "RTN":{
                     "Company":("fixed_value", "ST"),
                     "Plant":("fixed_value", "HEIM"),
                     "Area":("fixed_value", "SKR"),
                     "Unit":("fixed_value", ""),
                     "GPS":("fixed_value", ""),
                     "EventID":("value_by_position", 1),
                     "AlarmIdentifier": ("value_by_position", 12),
                     "TimeStamp":("value_by_position", 2),
                     "Tag": ("value_by_position", 9),
                     "TagDescription": ("value_by_position", 10),
                     "Message": ("value_by_position", 11),
                     "User1": ("value_by_position", 6),
                     "User2": ("value_by_position", 7),
                     "User3": ("value_by_position", 8),
                     "User4": ("value_by_position", 5),
                     "User5": ("value_by_position", 9),
                     "User7": ("value_by_position", 4),
                     "User8": ("value_by_position", 13),
                     "User9": ("value_by_position", 1),
                     "User10": ("value_by_position", 3)
                     },
            "OTHER":{
                     "Company":("fixed_value", "ST"),
                     "Plant":("fixed_value", "HEIM"),
                     "Area":("fixed_value", "SKR"),
                     "Unit":("fixed_value", ""),
                     "GPS":("fixed_value", ""),
                     "EventID":("value_by_position", 1),
                     "AlarmIdentifier": ("value_by_position", 12),
                     "TimeStamp":("value_by_position", 2),
                     "Tag": ("value_by_position", 9),
                     "TagDescription": ("value_by_position", 10),
                     "Message": ("value_by_position", 11),
                     "User1": ("value_by_position", 6),
                     "User2": ("value_by_position", 7),
                     "User3": ("value_by_position", 8),
                     "User4": ("value_by_position", 5),
                     "User5": ("value_by_position", 9),
                     "User7": ("value_by_position", 4),
                     "User8": ("value_by_position", 13),
                     "User9": ("value_by_position", 1),
                     "User10": ("value_by_position", 3)
                     },
            "ACK":{
                     "Company":("fixed_value", "ST"),
                     "Plant":("fixed_value", "HEIM"),
                     "Area":("fixed_value", "SKR"),
                     "Unit":("fixed_value", ""),
                     "GPS":("fixed_value", ""),
                     "EventID":("value_by_position", 1),
                     "AlarmIdentifier": ("value_by_position", 12),
                     "TimeStamp":("value_by_position", 2),
                     "Tag": ("value_by_position", 9),
                     "TagDescription": ("value_by_position", 10),
                     "Message": ("value_by_position", 11),
                     "User1": ("value_by_position", 6),
                     "User2": ("value_by_position", 7),
                     "User3": ("value_by_position", 8),
                     "User4": ("value_by_position", 5),
                     "User5": ("value_by_position", 9),
                     "User7": ("value_by_position", 4),
                     "User8": ("value_by_position", 13),
                     "User9": ("value_by_position", 1),
                     "User10": ("value_by_position", 3)
                     }
            }

# TODO: make generic, support custom formats
def get_event_type(data, raw_data):
    if data[8].find("Inactive"):
        return "RTN"
    elif data[8].find("Acked"):
        return "ACK"
    elif data[8].find("New"):
        return "ALARM"
    else: 
        return "OTHER"

# TODO: make generic, support custom formats
def get_basic_based_on_type(data, raw_data, event_type):
    # TODO: Make this a little more generic
    processed_event = {}

    for k, v in mappings[event_type].iteritems():
        processed_event[k] = possibles.get(v[0])(data, raw_data, v[1])
    
    event_id = processed_event["EventID"]
    
    return event_id, processed_event
    
    
def apply_adjustments(event, data, event_type):
    print "applying adjustments"

def proces_file_line(raw_line):
    
    uline = raw_line.encode("ascii", "ignore")
    # TODO: Extract line splitter
    data = uline.split("\t")
    
    # Detect message type
    event_type = get_event_type(data, uline)
    
    # Populate with data
    event_id, processed_event = get_basic_based_on_type(data, uline, event_type)
   
    # Apply adjustments - Old VBASIC Script    
    # apply_adjustments(processed_event, data, uline, event_type)
    
    # Return id - event tuple     
    return (event_id, processed_event)

if __name__ == "__main__":
    
    # TODO Check if these preparations can be skipped
    # create ES client, create index
    es = Elasticsearch(hosts=[ES_HOST])
    
    if es.indices.exists(INDEX_NAME):
        print("deleting '%s' index..." % (INDEX_NAME))
        res = es.indices.delete(index=INDEX_NAME)
        print(" response: '%s'" % (res))
    
    # since we are running locally, use one shard and no replicas
    request_body = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    
    print("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index=INDEX_NAME, body=request_body)
    print(" response: '%s'" % (res))
    

    # Define raw file
    raw_file = '/home/developer/scripts/testdata/audit.log'
    print "Input file: ", raw_file
    
    # Init Spark context
    print "Initiating Spark context"
    conf = SparkConf().setAppName("ESTest")
    sc = SparkContext(conf=conf)
    
    
    print "Loading input file"
    es_rdd = sc.textFile(raw_file)
    
    # TODO Add an extra operation and check if some lines have to be merged
    print "Processing input"
    processed_input = es_rdd.map(proces_file_line)
    # TODO: Check if this is executed in paralel
    
    
    print "Entering results in elasticsearch"
    processed_input.saveAsNewAPIHadoopFile(path='-',
                                           outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                                           keyClass="org.apache.hadoop.io.NullWritable",
                                           valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                                           conf=es_write_conf)
    
