# helper functions
import mimetypes
import re
import string
import json
import pandas as pd
from bson import ObjectId, DatetimeMS, json_util
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from datetime import datetime
from src.backend.sql.sql_query import *
from src.backend.predefined_list import *
from lxml import etree
from textblob import Word


# pip install -U spacy
# python -m spacy download en_core_web_sm
import spacy

# Load English tokenizer, tagger, parser and NER
nlp = spacy.load("en_core_web_sm")

ABBR_TREE = etree.parse("data/Abbreviations.xml")

def get_file_type(file_path):
    mime_type, encoding = mimetypes.guess_type(file_path)
    return mime_type

# helper function for uploading json file to MongoDb
# recursively convert {'$oid': '...'} to ObjectId
def convert_bson(value):
    if isinstance(value, dict) and '$oid' in value:
        return ObjectId(value['$oid'])
    elif isinstance(value, dict) and '$date' in value:
        value = pd.to_datetime(value['$date'])
        return DatetimeMS(value)
    elif isinstance(value, list):
        return [convert_bson(item) for item in value]
    elif isinstance(value, dict):
        return {key: convert_bson(val) for key, val in value.items()}
    return value

# function to separate phrase by upper case character
# e.g "unitPrice" to "unit price"
def separate_and_lowercase(text):
    separated_text = re.sub(r'(?<!^)(?=[A-Z])', ' ', text)
    return separated_text.lower()

# function to convert plural word to its singular form
def to_singular(word):
    if word.endswith('ss'):
        return word
    else:
        return Word(word).singularize()

# function to check the POS tagging of a word
# the word to check must be the first word in the input string 
# parameter: word, a string
# return value: the property of the first word in the string
# example: "total amount" ==> 'ADJ', the property of 'total' is adjective
def check_pos(word):
    if word.split()[0] == 'first':
        return 'ADJ'
    
    doc = nlp(word)[0]
    
    if doc.pos_ in ['ADJ', 'NOUN']:
        return doc.pos_
    elif doc.pos_ == 'VERB':
        if doc.tag_ in ['VB', 'VBP', 'VBZ']:
            return "VERB"
        elif doc.tag_ in ['VBD', 'VBN', 'VBG']:
            return "ADJ"

# tokenize_phrase
# 1. parameter: a **list** of string
# 2. words are all lower case after tokenized
# example: ['transaction_id', 'unitPrice'] ==> [['transaction', 'id'], ['unit', 'price]]
def tokenize_phrase(column_names):
    column_name_split = []
    for column_name in column_names:

        # remove punctuation in column names 
        for c in column_name:
            if c in string.punctuation:
                column_name = column_name.replace(c, ' ')

        # separate concatenated column names
        column_name = separate_and_lowercase(column_name)

        # separate column names to a list of words
        tokenize_column = column_name.split()

        # replace possible abbreviations with their full forms
        for i in range(len(tokenize_column)):
            results = ABBR_TREE.xpath(f'//div[li[text()="{tokenize_column[i]}"]]/text()')

            if len(results) != 0:
                tokenize_column[i] = results[0].split()[0]

        if 'i' in tokenize_column and 'd' in tokenize_column:
            tokenize_column = ['id' if x == 'i' else x for x in tokenize_column]
            tokenize_column.remove('d')

        column_name_split.append(tokenize_column)

    return column_name_split

def find_abbr(word):
    results = ABBR_TREE.xpath(f'//entry[index[@value="{word}"]]/div/li/text()')
    return results[0]

def get_temp_key(obj, column_name):
    name_split = tokenize_phrase([column_name])[0]

    concat = ' '.join(name_split)

    word_property = check_pos(concat)

    # if the column name only contains one word
    if len(name_split) == 1:
        temp_key = obj

    # if the first word of column name is an adj
    elif word_property == 'ADJ' or name_split[0].endswith('ing'):
        temp_key = obj

    # else the temp key is the first word in the column name
    else:
        temp_key = name_split[0]

    return temp_key

def map_to_integer(input_value):
    # Define a dictionary for mapping
    mapping = {
        "1": 1, "one": 1, "first": 1,
        "2": 2, "two": 2, "second": 2,
        "3": 3, "three": 3, "third": 3,
        "4": 4, "four": 4, "fourth": 4,
        "5": 5, "five": 5, "fifth": 5
    }
    
    # Convert input to lowercase string to handle case-insensitive matches
    input_str = str(input_value).lower()
    
    # Return the corresponding integer or None if not found
    return mapping.get(input_str, None)

def execute_sql_query(db_url, database,  query):
    try:
        database_name = database.name
        engine = create_engine(db_url)
        with engine.connect() as connection:
            connection.execute(text(f"USE {database_name}"))
            result = connection.execute(text(query))
            if query.strip().lower().startswith("select"):
                results = result.fetchall()
                keys = result.keys()  # Get column names
                formatted_results = [dict(zip(keys, row)) for row in results]
                return json.dumps(formatted_results, indent=4)
            else:
                return {"status": "Query executed successfully"}
    except Exception as e:
        print(f"Error executing query: {e}")
        return {"error": str(e)}


def handle_iso_date(filter_str):
    iso_date_matches = re.findall(r"ISODate\('([^']+)'\)", filter_str)
    for match in iso_date_matches:
        dt = datetime.fromisoformat(match.replace('Z', '+00:00'))  # Handle ISODate with timezone
        filter_str = filter_str.replace(f"ISODate('{match}')", f"datetime({dt.year}, {dt.month}, {dt.day}, {dt.hour}, {dt.minute}, {dt.second})")
    return filter_str


class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle MongoDB-specific data types."""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)  # Convert ObjectId to string
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO format
        return super().default(obj)  # Fallback to default

def normalize_aggregate_query(pipeline_str):
    stages = []
    current_stage = ""
    brace_count = 0

    for char in pipeline_str:
        if char == '{':
            brace_count += 1
        elif char == '}':
            brace_count -= 1

        current_stage += char

        if brace_count == 0 and current_stage.strip():
            stages.append(current_stage.strip().rstrip(','))
            current_stage = ""

    # Normalize each stage
    normalized_stages = []
    for stage in stages:
        if not stage.strip():
            continue  # Skip empty stages
        stage = re.sub(r'(\{|,)\s*(\$\w+):', r'\1"\2":', stage)  # Quote $ operators
        stage = re.sub(r'([\{,])\s*([a-zA-Z_]\w*)\s*:', r'\1"\2":', stage)  # Quote field names
        stage = stage.replace("'", '"')  # Replace single quotes with double quotes

        try:
            normalized_stages.append(json.loads(stage))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid aggregation stage format: {stage}. Error: {e}")

    return normalized_stages


def execute_nosql_query(db_url, database, query_string):
    client = MongoClient(db_url)
    db = client[database]
    collection_name = re.search(r'db\.(\w+)\.', query_string).group(1)
    operation = re.search(r'\.(\w+)\(', query_string).group(1)

    if not collection_name or not operation:
        raise ValueError("Invalid MongoDB query string.")

    collection = db[collection_name]

    if operation == "find":
        find_filter_match = re.search(r'\.find\((.*?)\)', query_string)
        find_filter_str = find_filter_match.group(1) if find_filter_match else '{}'
        find_filter_str = handle_iso_date(find_filter_str).replace("'", '"')
        find_filter = json.loads(find_filter_str)
        cursor = collection.find(find_filter)

        sort_match = re.search(r'\.sort\((.*?)\)', query_string)
        if sort_match:
            sort_criteria = json.loads(sort_match.group(1).replace("'", '"'))
            cursor = cursor.sort([(k, v) for k, v in sort_criteria.items()])

        limit_match = re.search(r'\.limit\((\d+)\)', query_string)
        if limit_match:
            cursor = cursor.limit(int(limit_match.group(1)))

        skip_match = re.search(r'\.skip\((\d+)\)', query_string)
        if skip_match:
            cursor = cursor.skip(int(skip_match.group(1)))

        results = list(cursor)

    elif operation == "aggregate":
        pipeline_match = re.search(r'\.aggregate\((.*?)\)', query_string, re.DOTALL)
        if pipeline_match:
            pipeline_str = pipeline_match.group(1).strip()
            pipeline = normalize_aggregate_query(pipeline_str)
            results = list(collection.aggregate(pipeline))
        else:
            raise ValueError("Invalid aggregation pipeline format.")

    elif operation == "countDocuments":
        count_filter_match = re.search(r'\.countDocuments\((.*?)\)', query_string)
        count_filter_str = count_filter_match.group(1) if count_filter_match else '{}'
        count_filter_str = handle_iso_date(count_filter_str).replace("'", '"')
        count_filter = json.loads(count_filter_str)
        results = collection.count_documents(count_filter)

    elif operation == "distinct":
        distinct_match = re.search(r'\.distinct\((.*?)\)', query_string)
        if distinct_match:
            distinct_match_content = distinct_match.group(1).replace("'", '"')
            distinct_args = json.loads(f"[{distinct_match_content}]")
        else:
            distinct_args = []

        results = collection.distinct(*distinct_args)

    else:
        raise ValueError(f"Unsupported operation: {operation}")

    return json.dumps(results, cls=JSONEncoder, indent=2)

def process_sql_query(input, database):
    user_input = input.lower()
    if any(word in user_input.split() for word in filter_words):
        queries, captions = generate_selected_sample_queries(input, database)
    else:
        queries, captions = interpret_user_input_generic(input,database)
    return queries, captions