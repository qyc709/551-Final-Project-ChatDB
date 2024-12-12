import json
from datetime import datetime, timezone
from src.backend.Table import *
from src.backend.helper import *
from src.backend.nosql.nosql_query_templates import *
from bson import json_util
import random
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

long_columns = ['time', 'detail', '_id', 'review', 'abstract', 'content', 'description', 'password']
avg_column = ['unit', 'rating', 'rate', 'speed']


# function to find the nested attribute in a dictionary, flattend the nested attributes and its data type
# parameters:
    # data: a python dictionary
    # parent_key: set default to ''
# return: a dictionary with key: all attribute names, value: data type of value stored.
# eg: {'items':{'productId': , 'qty', }} ==> {'items': 'dict', 'items.productId': type, 'items.qty': type}
def find_nested_attrs(data, parent_key=''):
    # Dictionary to hold the full path keys with their data types
    attrs = {}

    if isinstance(data, dict):
        for key, value in data.items():
            # Create the new key by concatenating the parent and current key, e.g., 'shippingAddress.city'
            new_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(value, list) and value:
                # If it's a list, check the type of the first element
                attrs[new_key] = 'list'
                attrs.update(find_nested_attrs(value[0], new_key))
            elif isinstance(value, dict):
                attrs[new_key] = 'dict'
                # If it's a dictionary, recurse into it
                attrs.update(find_nested_attrs(value, new_key))
            else:
                attrs[new_key] = type(value).__name__
    elif isinstance(data, list) and data:
        # If it's a list at the root level, recurse into the first element
        attrs.update(find_nested_attrs(data[0], parent_key))
    else:
        attrs[parent_key] = 'list'
        # attrs[parent_key] = type(data).__name__

    return attrs

def find_nested_column_types(table):
    data = table.df
    column_types = {}
    for column in data:
        column_types.update(find_nested_attrs({column:data[column][0]}))

    return column_types

# function to classify the column types to categorical and numeric
# parameter: column_types: a dictionary with key: column_name and value: type of value in the column
# return: two lists of column names: categorical and numeric
def classify_column_types(column_types):
    # classify columns to categorical or numeric
    categorical_columns = []
    numeric_columns = []

    for name, ctype in column_types.items():

        if any(word in name for word in long_columns):
            continue

        elif ctype in ['list', 'dict']:
            continue

        # classify 'id' column as categorical
        elif ctype == 'str':
            categorical_columns.append(name)

        elif 'date' in ctype.lower():
            categorical_columns.append(name)

        elif 'id' in tokenize_phrase([name])[0]:
            categorical_columns.append(name)

        elif 'int' in ctype or 'float' in ctype:
            # if '.' not in name:
                numeric_columns.append(name)
            
        else:
            continue

    # print(categorical_columns)
    # print(numeric_columns)

    return categorical_columns, numeric_columns


# function to get all possible values of a column after querying from mongodb
# parameter:
# data: a list of return documents from mongodb
# last_attr: the deepest attribute name. eg: items.productId ==> productId
def find_all_values(data, column):

    last_attr = column.split('.')[-1]

    possible_values = []
    if isinstance(data, list):
        for item in data:
            possible_values.extend(find_all_values(item, last_attr))
    elif isinstance(data, dict):
        for key, value in data.items():
            if key == last_attr:
                possible_values.append(value)
            else:
                possible_values.extend(find_all_values(value, last_attr))

    return possible_values


def find_valid_ccolumn(ncolumn, types, ccolumn_list):

    ccolumn = random.choice(ccolumn_list)
    
    nparent = find_root(ncolumn)
    cparent = find_root(ccolumn)
    # print("cparent: ", cparent, "\nnparent: ", nparent)

    nparent_split = nparent.split('.')
    cparent_split = cparent.split('.')

    if nparent != cparent:
        
        # if ccolumn is in the lower level of ncolumn
        if nparent and nparent in cparent:
            for i in nparent_split:
                cparent_split.pop(0)

        else:
            while nparent != '':
                # find the upper level parent
                nparent = find_root(nparent)
                nparent_split = nparent.split('.')

                # reach the same parent
                if nparent and nparent in cparent:
                    for i in nparent_split:
                        cparent_split.pop(0)
                    break
        
        while nparent != cparent:
            nparent = nparent + '.' + cparent_split[0] if nparent else cparent_split[0]
            cparent_split.pop(0)

            if types[nparent] == 'list':
                update_list = [value for value in ccolumn_list if value != ccolumn]
                ccolumn = find_valid_ccolumn(ncolumn, types, update_list)
                break
        
    return ccolumn


def get_sort_attrs(column_types, sort_list):
    # attrs = list(column_types.keys())
    attrs = [attr for attr in column_types if column_types[attr] != 'dict']
    # attrs = [attr for attr in attrs if column_types[attr] != 'dict']
    list_attrs = [key for key, value in column_types.items() if value in ['list']]
    for attr in attrs:
        in_list = False
        for list_attr in list_attrs:
            if list_attr in attr:
                in_list = True
                break

        if not in_list:
            sort_list.append(attr)

    return(sort_list)

# this function check if two tables can be joined
# parameter:
# tables_column_types: a dictionary with table_name is the key and types of all its column is the value
# return: a dictionary contains primary and foreign table and two corresponding join key
def check_valid_join(tables_column_types):
    table_names = list(tables_column_types.keys())

    table1 = table_names[0]
    table2 = table_names[1]

    id_attr1 = [attr for attr, type in tables_column_types[table1].items() if type == 'ObjectId' and attr !='_id']
    id_attr2 = [attr for attr, type in tables_column_types[table2].items() if type == 'ObjectId' and attr !='_id']

    lookup_component = {}
    if id_attr1:
        local_key = random.choice(id_attr1)
        lookup_key = local_key.split('.')[-1]
        lookup_key = tokenize_phrase([lookup_key])[0]
        lookup_key = ''.join([to_singular(word) for word in lookup_key if word != 'id'])

        if lookup_key in to_singular(table2):
            lookup_component['local'] = table1
            lookup_component['local_key'] = local_key
            lookup_component['foreign'] = table2
            lookup_component['foreign_key'] = '_id'

            return lookup_component

    if id_attr2:
        local_key = random.choice(id_attr2)
        lookup_key = local_key.split('.')[-1]
        lookup_key = tokenize_phrase([lookup_key])[0]
        lookup_key = ''.join([to_singular(word) for word in lookup_key if word != 'id'])

        if lookup_key in to_singular(table1):
            lookup_component['local'] = table2
            lookup_component['local_key'] = local_key
            lookup_component['foreign'] = table1
            lookup_component['foreign_key'] = '_id'

            return lookup_component
        
    for attr1 in tables_column_types[table1]:
        if attr1.lower() in ['_id', 'createdat', 'createat']:
            continue
        for attr2 in tables_column_types[table2]:
            if attr2.lower() in ['_id', 'createdat', 'createat']:
                continue
            if attr1.lower() == attr2.lower():
                lookup_component['local'] = table1
                lookup_component['local_key'] = attr1
                lookup_component['foreign'] = table2
                lookup_component['foreign_key'] = attr2

                return lookup_component

    return lookup_component



def format_query_value(column_type, value):
    if isinstance(value, list):
        return value
    
    if 'date' in column_type.lower():
        dt = json_util.default(value)
        dt = convert_bson(dt)
        dt = datetime.fromtimestamp(int(dt) / 1000, tz=timezone.utc)
        iso_date = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        value = f"ISODate('{iso_date}')"
    elif column_type == 'ObjectId':
        value = f"ObjectId('{value}')"
    elif 'int' in column_type or 'float' in column_type:
        value = value
    else:
        value = f'"{value}"'

    return value

def find_unwind_attrs(column_types, aggr_attr):
    # find attributes that need to be unwind first
    unwind_attrs = []
    ncolumn_split = aggr_attr.split('.')
    nparent = ncolumn_split[0]

    while nparent != aggr_attr:
        ncolumn_split.pop(0)
        if column_types[nparent] == 'list':
            new_unwind = unwind.format(unwind_attr=nparent)
            unwind_attrs.append(new_unwind)

        nparent = nparent + '.' + ncolumn_split[0]

    query = ''
    for unwind_syntax in unwind_attrs:
        query = query + unwind_syntax + ', ' if query else unwind_syntax + ', '

    return query

# function to format group part query 
# parameters:
# operator: key of the ari_operators dict(e.g. sum, count, avg)
# aggr_attr: the numeric column to be aggregated
# grouped_by: the categorical column to be grouped by
# return value: group query syntax {$group: ___}
def nosql_format_group_query(operator, aggr_attr, grouped_by):

    # give a name of the attribute after grouping
    temp_attr = tokenize_phrase([aggr_attr.split('.')[-1]])[0]
    temp_attr = '_'.join(temp_attr)
    temp_operator = operator.replace(' ', '_')
    aggr_title = f'{temp_operator}_{temp_attr}'

    # format aggregate syntax
    # count operation does not need to be format
    if operator != 'count':
        aggr_syntax = ari_operators[operator].format(attr=aggr_attr)
    else:
        aggr_syntax = ari_operators['count']

    # format the output syntax inside {$group: ___}
    group_syntax = f'{{"_id": "${grouped_by}", "{aggr_title}": {aggr_syntax}}}'
    group_syntax = group.format(group_query=group_syntax)

    return group_syntax, aggr_title


# attirbute name to be displayed in the caption
def format_caption_attr_name(column):
    if '.' in column:
        attr = column.split('.')

        caption_attr_name = ''
        for i in range(len(attr) - 1, -1, -1):
            tokenize_word = tokenize_phrase([attr[i]])[0]

            if 'id' in tokenize_word:
                tokenize_word = [word for word in tokenize_word if word != 'id']
                caption_attr_name = ' '.join(tokenize_word)
                break
            else:
                caption_attr_name = caption_attr_name + ' of the ' + ' '.join(tokenize_word) if caption_attr_name else ' '.join(tokenize_word)
    else:
        tokenize_word = tokenize_phrase([column])[0]
        tokenize_word = [word for word in tokenize_word if word != 'id']
        caption_attr_name = ' '.join(tokenize_word)    

    return caption_attr_name

def sample_query_dict(query, caption):
    sample_query = {}
    sample_query['caption'] = caption + ":"
    sample_query['query'] = query

    return sample_query

def find_root(column):
    return '.'.join(column.split('.')[:-1])

def print_nosql_queries(sample_queries):
    for sample_query in sample_queries:
        if sample_query['caption'] != None:
            print(f"{sample_query['num']}. {sample_query['caption']}")
        print(sample_query['query'], '\n')


def json_serializer(obj):
    if isinstance(obj, ObjectId):
        return f'{{"$oid": "{str(obj)}"}}'# Convert ObjectId to string
    if isinstance(obj, datetime):
        return f'{{ "$date": "{obj.strftime("%Y-%m-%dT%H:%M:%SZ")}" }}'
    
# function to get the first three documents of a table
# parameters:
# database: database that contains the table
# table: a single table (Table object) to get the sample data from
# return value: json string a the first three documents, in a json list format
def get_nosql_sample_data(database, table):
    db = client[database.name]

    collection = db[table.table_name]

    documents = collection.find().limit(3)
    documents = [document for document in documents]
    documents_json = json.dumps(documents, default=json_serializer, indent = 2)

    return documents_json


