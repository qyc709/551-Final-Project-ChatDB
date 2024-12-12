from src.backend.nosql.nosql_query_helper import *
from src.backend.nosql.nosql_query_templates import *
from src.backend.nosql.nosql_nlp_query import *
from src.backend.predefined_list import *
import random
import math
from pymongo import MongoClient

# function to find unique values of an attribute in mongodb
# return: a list of non-repetitive value
def nosql_get_unique_values(db_name, collection, attr):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]

    # extract all possible values of an attribute from mongodb
    collection = db[collection]
    projection = {attr: 1, "_id": 0}
    documents = collection.find({}, projection)
    documents = [document for document in documents]

    # randomly pick a possbile value to be in the example
    unique_values = list(set(find_all_values(documents, attr)))

    return unique_values

def pick_operation_value(operator, unique_values):
     # if $in operator was selected
    if operator in '$in':
        # randomly generate 2 sample values
        value = random.sample(unique_values, 2)

    # if $eq or $ne operator was selected
    elif operator in ['$eq', '$ne']:
        # randomly pick a value that exactly exists in the column
        value = random.choice(unique_values)

    else:
        min_value = min(unique_values)
        max_value = max(unique_values)
        #randomly pick a value in the range
        value = random.randint(math.ceil(min_value), math.ceil(max_value))

    return value


def nosql_append_addition(template_num, operator, sort_list):
    query = ''
    caption = ''
    if operator == 'sort':
        sort_attr = random.choice(sort_list)
        sort_order = random.choice([1, -1])
        sort_condition = f'{{"{sort_attr}": {sort_order}}}' 

        sort_query = ''
        if template_num in templates_set['find']:
            sort_query = find_sort.format(condition=sort_condition)
            

        elif template_num in templates_set['aggregate'] + templates_set['lookup']:
            sort_query = agg_sort.format(condition=sort_condition)
        
        query = sort_query

        if sort_attr == '_id':
            caption = f" sorted by '_id'"
        else:
            caption = f' sorted by {format_caption_attr_name(sort_attr)}'
        
    elif operator == 'limit':
        num = random.choice([1, 2, 3])
        limit_query = ''
        if template_num in templates_set['find']:
            limit_query = find_limit.format(num=num)
            

        elif template_num in templates_set['aggregate'] + templates_set['lookup']:
            limit_query = agg_limit.format(num=num)

            
        query = limit_query
        caption = f' limit {num}'

    elif operator == 'skip':
        num = random.choice([1, 2, 3])

        skip_query = ''
        if template_num in templates_set['find']:
            skip_query = find_skip.format(num=num)

        elif template_num in templates_set['aggregate'] + templates_set['lookup']:
            skip_query = agg_skip.format(num=num)

        query = skip_query
        caption = f' skip {num}'

    return query, caption


def generate_nosql_sample_query(database, template_num, required_operators):
    
    # get the full structure of the template by the given template num
    query_template = next((template for template in nosql_query_templates if template['num'] == template_num), None)

    # use the column types that the query required to select column
    query_type = query_template['query_type']

    lookup_com = {}
    tables_column_types = {}
    if query_type == 'lookup':
        selected_tables = random.sample(database.tables, 2)

        for table in selected_tables:
            tables_column_types[table.table_name] = find_nested_column_types(table)

        lookup_com = check_valid_join(tables_column_types)

        while not lookup_com:
            selected_tables = random.sample(database.tables, 2)
            
            tables_column_types = {}
            for table in selected_tables:
                tables_column_types[table.table_name] = find_nested_column_types(table)

            lookup_com = check_valid_join(tables_column_types)

    else:
        selected_tables = random.sample(database.tables, 1)
        column_types = find_nested_column_types(selected_tables[0])
        categorical_columns, numeric_columns = classify_column_types(column_types)

        while (query_type == 'numeric' or 'aggregate' in query_type) and not numeric_columns:
            selected_tables = random.sample(database.tables, 1)
            column_types = find_nested_column_types(selected_tables[0])
            categorical_columns, numeric_columns = classify_column_types(column_types)

        tables_column_types[selected_tables[0].table_name] = column_types


    # define a sample query, this will be the return value
    sample_query = {}

    # if sort is required, pick an attribute to sort by
    sort_list = []
    if 'sort' in required_operators:
        for key, column_types in tables_column_types.items():
            sort_list = get_sort_attrs(column_types, sort_list)
    
    # generating sample queries using categorical columns
    if query_type == 'categorical': 
        table = selected_tables[0]
        column_types = tables_column_types[table.table_name]

        categorical_columns, numeric_columns = classify_column_types(column_types)

        categorical_columns = categorical_columns + numeric_columns
        column = random.choice(categorical_columns)

        # extract all possible values of an attribute from mongodb
        unique_values = nosql_get_unique_values(database.name, table.table_name, column)
        value = random.choice(unique_values)
        
        # format value in the syntax
        value = format_query_value(column_types[column], value)

        query_syntax = query_template['syntax'].format(table=table.table_name, column=column, value=value)

        # format output key in the caption
        caption_key = get_temp_key(table.table_name, table.primary_key)

        # format attribute name to be displayed in the caption
        caption_attr_name = format_caption_attr_name(column)

        caption = query_template['caption'].format(
            key=caption_key,
            column=caption_attr_name
        )

        for operator in required_operators:
            add_query, add_caption =  nosql_append_addition(template_num, operator, sort_list)
            query_syntax = query_syntax + add_query
            caption = caption + add_caption

        sample_query = sample_query_dict(query_syntax, caption)

    # generating sample queries using numeric columns
    elif query_type == 'numeric':
        table = selected_tables[0]
        column_types = tables_column_types[table.table_name]

        categorical_columns, numeric_columns = classify_column_types(column_types)

        column = random.choice(numeric_columns)
        operator = random.choice(list(comp_operators.items()))

        # print("numeric_column: ", column)
        # print("ccolumn: ", categorical_column)

        unique_values = nosql_get_unique_values(database.name, table.table_name, column)

        value = pick_operation_value(operator[1], unique_values)

        query_syntax = query_template['syntax'].format(
            table=table.table_name,
            column=column,
            value=value,
            operator=operator[1]
        )

        # format output key in the caption
        caption_key = get_temp_key(table.table_name, table.primary_key)

        # format attribute name to be displayed in the caption
        caption_attr_name = format_caption_attr_name(column)

        caption = query_template['caption'].format(
            key=caption_key,
            column=caption_attr_name,
            operator=operator[0],
            value = value
        )

        for operator in required_operators:
            add_query, add_caption =  nosql_append_addition(template_num, operator, sort_list)
            query_syntax = query_syntax + add_query
            caption = caption + add_caption

        # format sample query caption
        sample_query = sample_query_dict(query_syntax, caption)

    elif 'aggregate' in query_type:
        # randomly choose a categorical and a numeric column

        table = selected_tables[0]
        column_types = tables_column_types[table.table_name]

        categorical_columns, numeric_columns = classify_column_types(column_types)
        
        numeric_column = random.choice(numeric_columns)
        categorical_column = find_valid_ccolumn(numeric_column, column_types, categorical_columns)

        # print("numeric_column: ", numeric_column)
        # print("ccolumn: ", categorical_column)

        # if the template type is aggregate count
        # select count as the aggr_operator
        if 'count' in query_type:
            aggr_operator = 'count'

        # unit column can only use '$avg' operator
        elif any(word in numeric_column for word in avg_column):
            aggr_operator = [key for key in ari_operators if key == 'average']
            aggr_operator = aggr_operator[0]

        # if the result will be sorted, do not choose max and min as the aggr_operator
        elif 'sort' in required_operators:
            avai_operators = [key for key in ari_operators if key not in ['highest', 'least']]
            aggr_operator = random.choice(avai_operators)

        # else randomly choose an aggregate operator
        else:
            avai_operators = [key for key in ari_operators if key != 'count']
            aggr_operator = random.choice(avai_operators)

        query = find_unwind_attrs(column_types, numeric_column)
        group_syntax, aggr_title = nosql_format_group_query(aggr_operator, categorical_column, numeric_column)

        # format projection to round avg to 2 decimal places
        if 'average' in group_syntax:
            proj = f'"{aggr_title}": {{"$round": ["${aggr_title}", 2]}}'
        else:
            proj = f'"{aggr_title}": 1'

        proj_syntax = project.format(ccolumn=categorical_column, projection=proj)
        

        # format caption
        caption_column = categorical_column.split('.')[-1]
        caption_grouped_by = format_caption_attr_name(caption_column)
        caption_aggr_attr = format_caption_attr_name(numeric_column)

        caption = query_template['caption'].format(operator=aggr_operator, aggr_field=caption_aggr_attr, column=caption_grouped_by)
        
        # add sort, limit, skip, syntax and format
        add_syntax = ''
        for operator in required_operators:
            if operator == 'sort':
                sort_list.append(aggr_title)

            add_query, add_caption =  nosql_append_addition(template_num, operator, sort_list)
            add_syntax = add_syntax + add_query
            caption = caption + add_caption

        query = query + group_syntax + proj_syntax + add_syntax
        
        query_syntax = query_template['syntax'].format(table=table.table_name, query=query)

        sample_query = sample_query_dict(query_syntax, caption)

    elif query_type == 'lookup':
        lookup_syntax = lookup.format(foreign=lookup_com['foreign'], local=lookup_com['local_key'], foreign_key=lookup_com['foreign_key'])

        unwind_syntax = unwind.format(unwind_attr='tmp')

        column_types = tables_column_types[lookup_com['foreign']]
        categorical_columns, numeric_columns = classify_column_types(column_types)
        
        match_attr = random.choice(categorical_columns + numeric_columns)

        # extract all possible values of an attribute from mongodb
        unique_values = nosql_get_unique_values(database.name, lookup_com['foreign'], match_attr)

        # randomly choose to use operator or not
        using_operator = False 
        operator = None
        if match_attr in numeric_columns: 
            using_operator = random.choice([True, False])
            if using_operator:
                operator = random.choice(list(comp_operators.items()))

        if using_operator:
            value = pick_operation_value(operator[1], unique_values)
            value = format_query_value(column_types[match_attr], value)
            match_condition = f'{{"tmp.{match_attr}": {{{operator[1]}: {value}}}}}'
            operator_name=operator[0]

        else: 
            value = random.choice(unique_values)
            value = format_query_value(column_types[match_attr], value)
            match_condition = f'{{"tmp.{match_attr}": {value}}}'

            operator_name = '='

        # format value in the syntax
        match_syntax = match.format(conditions=match_condition)

        # format caption
        for table in selected_tables:
            if table.table_name == lookup_com['local']:
                primary_key = table.primary_key

        # format output key in the caption
        caption_key = get_temp_key(lookup_com['local'], primary_key)

        # format attribute name to be displayed in the caption
        caption_attr_name = format_caption_attr_name(match_attr)    

        caption = query_template['caption'].format(
                key=caption_key,
                column=caption_attr_name,
                operator=operator_name,
                value = value
            )
        

        # add sort, limit, skip, syntax and format
        add_syntax = ''
        for operator in required_operators:
            if operator == 'sort':
                sort_list = get_sort_attrs(tables_column_types[lookup_com['local']], sort_list=[])
                foreign_sort = get_sort_attrs(tables_column_types[lookup_com['foreign']], sort_list=[])

                for sort_key in foreign_sort:
                    sort_list.append('tmp.' + sort_key)

            add_query, add_caption = nosql_append_addition(template_num, operator, sort_list)
            add_syntax = add_syntax + add_query
            caption = caption + add_caption

        query = lookup_syntax + ', ' + unwind_syntax + ', ' + match_syntax + add_syntax

        query_syntax = query_template['syntax'].format(table=lookup_com['local'], query=query) 
        sample_query = sample_query_dict(query_syntax, caption)
        

    return sample_query



def get_nosql_sample_queries(database, available_templates, required_operators):
    # this list contains sample query generated
    # elements in the list are dictionary
    # content of dictionary: {'num': query number, 'template': template number, 'caption': caption to be displayed, 'query': query syntax}
    sample_query_list = []

    template_nums = []
    
    #available_templates = [8]
    
    # generating 5 sample queries
    for i in range(5):
        # randomly choose a query template
        temp_operators = required_operators[:]

        template_num = random.choice(available_templates)

        if len(available_templates) > 4:
            while template_nums.count(template_num) >= 1:
                available_templates = [template for template in available_templates if template != template_num]
                template_num = random.choice(available_templates)
        elif len(available_templates) == 1:
            template_num = random.choice(available_templates)
        else:
            while template_nums.count(template_num) > 2:
                available_templates = [template for template in available_templates if template != template_num]
                template_num = random.choice(available_templates)


        template_nums.append(template_num)

        if not temp_operators and template_num in [1, 2, 6, 7, 8]:
            add_operator = random.choice([True, False])
            if add_operator:
                operator = random.choice(['sort', 'skip', 'limit'])
                temp_operators.append(operator)
        
        sample_query = generate_nosql_sample_query(database, template_num, temp_operators)

        # record template number
        sample_query['num'] = i + 1
        sample_query['template_num'] = template_num
        sample_query_list.append(sample_query)

    return sample_query_list


