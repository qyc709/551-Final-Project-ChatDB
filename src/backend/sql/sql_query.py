import random
import re
import string
import pandas as pd
from lxml import etree
from textblob import Word
import spacy
from sqlalchemy import create_engine, text
import json
import src.backend.setup as setup

# adjust query templates
# datatype of var and refine query template
database = {
    "coffee_sales": {"product_category":["describe1", "categorical"], "transaction_qty":["describe2", "integer"], "unit_price":["describe3", "float"], "store_location":["describe4", "categorical"]}
}

def fetch_table_preview(db_url, database, table):
    # Database connection URL (configure as needed)
    engine = create_engine(db_url)
    database_name = database.name
    table_name = table.table_name
    with engine.connect() as connection:
            # Use the specified database
            connection.execute(text(f"USE {database_name};"))

            # Fetch the first three rows and their column names
            preview_query = text(f"SELECT * FROM {table_name} LIMIT 3;")
            result = connection.execute(preview_query)
            rows = result.fetchall()
            column_names = result.keys()

            # Convert rows to a list of dictionaries
            data = [dict(zip(column_names, row)) for row in rows]

            # Return the data as a JSON string
            return json.dumps(data, indent=4)

query_templates = [
    {
        "caption": "Find {primary}, {categorical}",
        "query": "SELECT {primary}, {categorical} FROM {table} LIMIT 3"
    },
    {
        "caption": "Find {columns} and {agg_func} {agg_column}, where the output is grouped by {group_by}",
        "query": "SELECT {columns}, {agg_func}({agg_column}) FROM {table} GROUP BY {group_by} LIMIT 3"
    },
    {
        "caption": "Find {columns}, which is ordered by {order_by}",
        "query": "SELECT {columns} FROM {table} ORDER BY {order_by} LIMIT 3"
    },
    {
        "caption": "Find {categorical} and {numerical}, where {where_col} is {operator_int} {num}",
        "query": "SELECT {categorical}, {numerical} FROM {table} WHERE {where_col} {operator} {num} LIMIT 3"
    },
    {
        "caption": "Find unique {categorical}",
        "query": "SELECT DISTINCT({categorical}) FROM {table} LIMIT 3"
    },
    {
        "caption": "Find {columns} and {agg_func} {agg_column}, where the output is grouped by {group_by} and {where_col} is {operator_int} {num}",
        "query": "SELECT {columns}, {agg_func}({agg_column}) FROM {table} WHERE {where_col} {operator} {num} GROUP BY {group_by} LIMIT 3"
    }
]
# find product type, unit price where unit price is greater than 24

ABBR_TREE = etree.parse("data/Abbreviations.xml")

def separate_and_lowercase(text):
    text = re.sub(r'_', ' ', text)
    separated_text = re.sub(r'(?<!^)(?=[A-Z])', ' ', text)
    return separated_text.lower()

def tokenize_phrase(column_names):
    column_name_split = []
    for column_name in column_names:

        column_name = ''.join([c if c not in string.punctuation else ' ' for c in column_name])

        column_name = separate_and_lowercase(column_name)
        
        if ' ' not in column_name:  
            tokenize_column = [column_name]
        else:
            tokenize_column = column_name.split()

        
        for i in range(len(tokenize_column)):
            results = ABBR_TREE.xpath(f'//div[li[text()="{tokenize_column[i]}"]]/text()')

            if results: 
                tokenize_column[i] = results[0].split()[0]

        column_name_split.append(tokenize_column)

    flattened_column_names = [" ".join(tokens) for tokens in column_name_split]
    return ", ".join(flattened_column_names)  # Join all names with commas into a single string

def random_query(database):
    tables = database.tables
    table_names=[]
    columns=[]
    datatypes=[]

    for i in tables:
        table_names.append(i.table_name)
        columns.append(i.extract_columns())
        datatypes.append(i.set_column_types())

    table_name=table_names[0]
    columns=columns[0]
    datatype=datatypes[0]

    template = random.choice(query_templates)
    
    id_columns= [col for col in columns if 'id' in col.lower()]
    non_id_columns = [col for col in columns if col not in id_columns]
    numeric_columns = [col for col, desc in datatype.items() if desc in ['integer64', 'float64'] and col not in id_columns]
    categorical_columns = [col for col in non_id_columns if col not in numeric_columns]
    primary = random.sample(id_columns, 1)[0]
    categorical = random.sample(categorical_columns, 1)[0]
    numerical = random.sample(numeric_columns, 1)[0]
    where_col = random.sample(numeric_columns, 1)[0]
    num = random.randint(1, 100)

    agg_funcs=['MIN','MAX','SUM','COUNT','AVG']
    agg_func=random.choice(agg_funcs)
    agg_column= None
    

    if "COUNT" == agg_func:
        agg_column = random.choice(categorical_columns)
    elif numeric_columns:
        agg_column = random.choice(numeric_columns)

    columns_to_remove = []
    if agg_column:
        columns_to_remove.append(agg_column)

    column = random.sample([col for col in categorical_columns if col not in columns_to_remove], 1)[0]

    order_by = random.choice([categorical_columns]+[id_columns])[0]

    operators={'>':"greater than",'<': 'less than', '=':'equal to', '>=':'greater than and equal to', '<=': 'less than and equal to'}
    operator = random.choice(list(operators.keys()))
    operator_int = operators[operator]
    
    query = template['query'].format(
        primary=primary,
        categorical=categorical,
        columns=column,
        table=table_name,
        agg_func=agg_func,
        agg_column=agg_column,
        group_by=column,
        order_by=order_by,
        where_col=where_col,
        num=num,
        operator=operator,
        numerical=numerical
    )

    primary_format=tokenize_phrase([primary])
    categorical_format=tokenize_phrase([categorical])
    column_format=tokenize_phrase([column])
    agg_column_format=tokenize_phrase([agg_column])
    order_by_format=tokenize_phrase([order_by])
    where_col_format=tokenize_phrase([where_col])
    numerical_format=tokenize_phrase([numerical])

    if agg_func=="MAX":
        agg_func_format='the maximum of'
    elif agg_func=="MIN":
        agg_func_format='the minimum of'
    elif agg_func=="COUNT":
        agg_func_format='count the occurence of'
    elif agg_func=="AVG":
        agg_func_format='the average of'
    elif agg_func=="SUM":
        agg_func_format='the total of'

    caption = template['caption'].format(
        primary=primary_format,
        categorical=categorical_format,
        columns=column_format,
        table=table_name,
        agg_func=agg_func_format,
        agg_column=agg_column_format,
        group_by=column_format,
        order_by=order_by_format,
        where_col=where_col_format,
        num=num,
        operator_int=operator_int,
        numerical=numerical_format
    )

    return query, caption

def generate_sample_queries(database):
    queries = []
    captions = []
    
    for i in range(5):
        query,caption = random_query(database)
        queries.append(query)
        captions.append(caption)
    
    return queries, captions

def selected_random_query(user_input, database):
    if re.search(r'\b(group by|groupby)\b', user_input.lower(), re.IGNORECASE) and re.search(r'\b(where|if)\b', user_input.lower(), re.IGNORECASE):
        template = query_templates[5]
    elif re.search(r'\b(group by|groupby)\b', user_input.lower(), re.IGNORECASE):
        # If found, select a relevant query template that contains 'GROUP BY'
        template = query_templates[1]
    elif re.search(r'\b(order by|orderby)\b', user_input.lower(), re.IGNORECASE):
        template = query_templates[2]
    elif re.search(r'\b(where|if)\b', user_input.lower(), re.IGNORECASE):
        template = query_templates[3]
    elif re.search(r'\b(distinct|unique|only)\b', user_input.lower(), re.IGNORECASE):
        template = query_templates[4]
    elif re.search(r'\b(and)\b', user_input.lower(), re.IGNORECASE):
        template = query_templates[0]
    else:
        return random_query(database)
    
    tables = database.tables
    table_names=[]
    columns=[]
    datatypes=[]

    for i in tables:
        table_names.append(i.table_name)
        columns.append(i.extract_columns())
        datatypes.append(i.set_column_types())

    table_name=table_names[0]
    columns=columns[0]
    datatype=datatypes[0]
    
    id_columns= [col for col in columns if 'id' in col.lower()]
    non_id_columns = [col for col in columns if col not in id_columns]
    numeric_columns = [col for col, desc in datatype.items() if desc in ['integer64', 'float64'] and col not in id_columns]
    categorical_columns = [col for col in non_id_columns if col not in numeric_columns]

    primary = random.sample(id_columns, 1)[0]
    categorical = random.sample(categorical_columns, 1)[0]
    numerical = random.sample(numeric_columns, 1)[0]
    where_col = random.sample(numeric_columns, 1)[0]
    num = random.randint(1, 100)

    agg_funcs=['MIN','MAX','SUM','COUNT','AVG']
    agg_func=random.choice(agg_funcs)
    agg_column= None
    

    if "COUNT" == agg_func:
        agg_column = random.choice(categorical_columns)
    elif numeric_columns:
        agg_column = random.choice(numeric_columns)

    columns_to_remove = []
    if agg_column:
        columns_to_remove.append(agg_column)

    column = random.sample([col for col in categorical_columns if col not in columns_to_remove], 1)[0]

    order_by = random.choice([categorical_columns]+[id_columns])[0]

    operators={'>':"greater than",'<': 'less than', '=':'equal to', '>=':'greater than and equal to', '<=': 'less than and equal to'}
    operator = random.choice(list(operators.keys()))
    operator_int = operators[operator]
    
    query = template['query'].format(
        primary=primary,
        categorical=categorical,
        columns=column,
        table=table_name,
        agg_func=agg_func,
        agg_column=agg_column,
        group_by=column,
        order_by=order_by,
        where_col=where_col,
        num=num,
        operator=operator,
        numerical=numerical
    )

    primary_format=tokenize_phrase([primary])
    categorical_format=tokenize_phrase([categorical])
    column_format=tokenize_phrase([column])
    agg_column_format=tokenize_phrase([agg_column])
    order_by_format=tokenize_phrase([order_by])
    where_col_format=tokenize_phrase([where_col])
    numerical_format=tokenize_phrase([numerical])

    if agg_func=="MAX":
        agg_func_format='the maximum of'
    elif agg_func=="MIN":
        agg_func_format='the minimum of'
    elif agg_func=="COUNT":
        agg_func_format='count the occurence of'
    elif agg_func=="AVG":
        agg_func_format='the average of'
    elif agg_func=="SUM":
        agg_func_format='the total of'

    caption = template['caption'].format(
        primary=primary_format,
        categorical=categorical_format,
        columns=column_format,
        table=table_name,
        agg_func=agg_func_format,
        agg_column=agg_column_format,
        group_by=column_format,
        order_by=order_by_format,
        where_col=where_col_format,
        num=num,
        operator_int=operator_int,
        numerical=numerical_format
    )

    return query, caption

def generate_selected_sample_queries(input, database):
    queries = []
    captions = []
    
    for i in range(5):
        query,caption = selected_random_query(input, database)
        queries.append(query)
        captions.append(caption)
    
    return queries, captions

def interpret_user_input_generic(user_input, database):
    tables = database.tables
    table_names=[]
    columns=[]
    datatypes=[]

    for i in tables:
        table_names.append(i.table_name)
        columns.append(i.extract_columns())
        datatypes.append(i.set_column_types())
        
    table_name=table_names[0]
    columns=columns[0]
    datatype=datatypes[0]

    captions = []
    queries = []

    user_input = user_input.lower()
    
    query_components = {
        "primary": None,
        "categorical": None,
        "columns": None, 
        "table": table_name,
        "agg_func": None,
        "agg_column": None,
        "group_by": None,
        "order_by": None,
        "where_col": None,
        "num": None, ###
        "operator": None,###
        "numerical": None
    }

    tokenized_columns = tokenize_phrase(columns).split(", ")
    columns_map = {tokenized: original for tokenized, original in zip(tokenized_columns, columns)}
    matched_columns = [columns_map[tokenized] for tokenized in columns_map if tokenized in user_input]

    id_columns= [col for col in columns if 'id' in col.lower()]
    non_id_columns = [col for col in columns if col not in id_columns]
    numeric_columns = [col for col, desc in datatype.items() if desc in ['integer64', 'float64'] and col not in id_columns]
    categorical_columns = [col for col in non_id_columns if col not in numeric_columns]

    id_columns_update=[]
    numeric_columns_update=[]
    categorical_columns_update=[]

    for column in matched_columns:
        if column in id_columns:
            id_columns_update.append(column)
        elif column in numeric_columns:
            numeric_columns_update.append(column)
        elif column in categorical_columns:
            categorical_columns_update.append(column)
    
    if id_columns_update:
        query_components["primary"] = id_columns_update[0]
    if categorical_columns_update:
        query_components["categorical"] = categorical_columns_update[0]

    for tokenized, original in columns_map.items():
        if f"where {tokenized}" in user_input or f"if {tokenized}" in user_input or f"with {tokenized}" in user_input:
            query_components["where_col"] = original
            break

    remaining_numerical_cols = [original for tokenized, original in columns_map.items() if original != query_components.get("where_col")]
    query_components["numerical"] = remaining_numerical_cols[0]
    
    for tokenized, original in columns_map.items():
        if f"group by {tokenized}" in user_input or f"groupby {tokenized}" in user_input:
            query_components["group_by"] = original
            break

    for col in columns_map.keys():
        if f"min {col.lower()}" in user_input or f"minimum {col.lower()}" in user_input:
            query_components["agg_func"] = 'MIN'
            query_components["agg_column"]=columns_map[col]
        elif f"max {col.lower()}" in user_input or f"maximum {col.lower()}" in user_input:
            query_components["agg_func"] = 'MAX'
            query_components["agg_column"]=columns_map[col]
        elif f"count {col.lower()}" in user_input or f"cnt {col.lower()}" in user_input:
            query_components["agg_func"] = 'COUNT'
            query_components["agg_column"]=columns_map[col]
        elif f"avg {col.lower()}" in user_input or f"average {col.lower()}" in user_input:
            query_components["agg_func"] = 'AVG'
            query_components["agg_column"]=columns_map[col]
        elif f"sum {col.lower()}" in user_input or f"total {col.lower()}" in user_input:
            query_components["agg_func"] = 'SUM'
            query_components["agg_column"]=columns_map[col]
    
    for col in columns_map.keys():
        if f"order by {col.lower()}" in user_input or f"orderby {col.lower()}" in user_input:
            order_by = columns_map[col]
            query_components["order_by"] = order_by
            break

    columns_to_remove = []
    if query_components["agg_column"]:
        columns_to_remove.append(query_components["agg_column"])
    columns_to_remove_set = set(columns_to_remove)  # Convert to set for efficient lookups

    query_components["columns"] = matched_columns
    query_components["columns"] = list(set(query_components["columns"]) - columns_to_remove_set)

    query_components["columns"] = ", ".join(query_components["columns"])

    operators={'>':"greater than",'<': 'less than', '=':'equal to', '>=':'greater than and equal to', '<=': 'less than and equal to'}

    if "greater than and equal to" in user_input:
        query_components["operator"] = '>='
    elif "less than and equal to" in user_input:
        query_components["operator"] = '<='
    elif "equal to" in user_input:
        query_components["operator"] = '='
    elif "greater than" in user_input:
        query_components["operator"] = '>'
    elif "less than" in user_input:
        query_components["operator"] = '<'

    match_num = re.finditer(r'-?\b\d+(\.\d+)?\b', user_input)
    if match_num:
        for match in match_num:
            query_components["num"]=match.group()


    if re.search(r'\b(group by|groupby)\b', user_input.lower(), re.IGNORECASE) and re.search(r'\b(where|if|with|greater|bigger|larger|smaller|less|fewer)\b', user_input.lower(), re.IGNORECASE):
        queries.append(query_templates[5]["query"].format(**query_components))
        return queries, captions
    elif re.search(r'\b(group by|groupby)\b', user_input.lower(), re.IGNORECASE) and re.search(r'\bfind\b', user_input.lower(), re.IGNORECASE):
        queries.append(query_templates[1]["query"].format(**query_components))
        return queries, captions
    elif re.search(r'\b(order by|orderby)\b', user_input.lower(), re.IGNORECASE) and re.search(r'\bfind\b', user_input.lower(), re.IGNORECASE):
        queries.append(query_templates[2]["query"].format(**query_components))
        return queries, captions
    elif re.search(r'\b(where|if|with|greater|bigger|larger|smaller|less|fewer)\b', user_input.lower(), re.IGNORECASE) and re.search(r'\bfind\b', user_input.lower(), re.IGNORECASE):
        queries.append(query_templates[3]["query"].format(**query_components))
        return queries, captions
    elif re.search(r'\b(distinct|unique|only)\b', user_input.lower(), re.IGNORECASE) and re.search(r'\bfind\b', user_input.lower(), re.IGNORECASE):
        queries.append(query_templates[4]["query"].format(**query_components))
        return queries, captions
    elif user_input is not None and re.search(r'\bfind\b', user_input.lower(), re.IGNORECASE):
        queries.append(query_templates[0]["query"].format(**query_components))
        return queries, captions
    else:
        return random_query(database)
    
# Generate queries
table = "coffee_sales"
columns = ["product_category", "transaction_qty", "unit_price", "store_location"]
datatype = {"product_category": "categorical", "transaction_qty": "integer64", "unit_price": "float64", "store_location": "categorical"}

# # queries_selected, captions_selected = generate_selected_sample_queries('example with cnt', table, columns, datatype)
# # print_queries(queries_selected, captions_selected)

# queries, captions = generate_sample_queries(table, columns, datatype)
# print_queries(queries, captions)

# user_input = "Find transaction quantity."
# sql_query = interpret_user_input_generic(user_input, columns, table)

# print(sql_query)