from src.backend.nosql.nosql_query_templates import *
from src.backend.nosql.nosql_query_helper import *
from src.backend.helper import *


def nosql_attrs_function_match(column_types, attrs, attrs_token, input_partition):

    categorical_columns, numeric_columns = classify_column_types(column_types)
    user_input = ' '.join(input_partition)

    operations = {}

    for i in range(len(input_partition)):
        next_partition = False

        input_partition[i] = input_partition[i].lower()
        ##### Find if **aggregate operation** is needed 
        for key, value in aggr_types.items():
            for word in value:
                if word in input_partition[i]:
                    operations['aggr_type'] = key

                    if attrs[i] in categorical_columns or 'id' in attrs_token[i]: 
                        operations['grouped_by'] = attrs[i]

                    if attrs[i] in numeric_columns:
                        operations['aggr_attr'] = attrs[i]
                    elif attrs[i] in categorical_columns and key == 'count':
                        operations['aggr_attr'] = attrs[i]

                    next_partition = True

            # only find the first matched aggregate type
            if next_partition:
                next_partition = False
                break

        ##### if **aggregate operation** is needed, find grouped by attribute
        # if no aggregate, this operation will be ignored later         
        for word in if_group:
            if word in input_partition[i]:
                if attrs[i] in categorical_columns or 'id' in attrs_token[i]:  
                    operations['grouped_by'] = attrs[i] 
                elif attrs[i] in numeric_columns:
                    operations['aggr_attr'] = attrs[i]


        ##### find if **sort operation** is needed and the sort attribute
        for key, value in if_sort.items():
            for word in value:
                if word in input_partition[i]:
                    operations['sort_by'] = attrs[i]
                    operations['sort_order'] = key
                    next_partition = True


        ##### find if **limit operation** is needed and the limit number
        for key, value in if_limit.items():
            for word in value:
                if word in input_partition[i]:
                    if key == '1':
                        operations['sort_by'] = attrs[i]
                        operations['sort_order'] = int(key)
                        operations['limit'] = 1
                    elif key == '-1':
                        operations['sort_by'] = attrs[i]
                        operations['sort_order'] = int(key)
                        operations['limit'] = 1
                    elif key == 'top':
                        number = int(re.search(r'\d+', input_partition[i]).group())
                        operations['sort_by'] = attrs[i]
                        operations['sort_order'] = -1
                        operations['limit'] = number
                    elif key == 'limit':
                        number = int(re.search(r'\d+', input_partition[i]).group())
                        operations['limit'] = number

                    next_partition = True

        ##### find if **skip operation** is needed and the limit number       
        for word in if_skip: 
            if word in input_partition[i]:
                number = int(re.search(r'\d+', input_partition[i]).group())
                operations['skip'] = number

                next_partition = True


        # if the attribute is used to sort and limit the output, do not need to go to next step
        if next_partition:
            continue

        ##### find if **limit operation** is needed, the filtered operator and value
        for key, value in if_filter.items():
            for word in value:
                if word in input_partition[i]:
                    operations['filter_attr'] = attrs[i]  
                    operations['filter_type'] = key

                    if attrs[i] in numeric_columns:
                        if re.search(r'\d+', input_partition[i]):
                            number = float(re.search(r'\d+', input_partition[i]).group())
                            operations['filter_value'] = number
                    elif attrs[i] in categorical_columns:
                        if 'e.g.' in user_input:
                            value = user_input.split('e.g.')[1]
                        else:
                            value = input_partition[i].split(word)[1]
                        value = value.strip(string.punctuation + " ")
                        operations['filter_value'] = value
                    
                    next_partition = True
                    # break

            # only find the first matched filter type
            if next_partition:
                next_partition = False 
                break

    return operations


def nosql_format_nlquery(database, operations):

    table = database.tables[0]

    # find the types of all attributes
    column_types = find_nested_column_types(table)
    categorical_columns, numeric_columns = classify_column_types(column_types)

    aggr_type = operations['aggr_type']
    if aggr_type == 'no_aggregate':
        frame = 'find' 
    elif aggr_type == 'distinct':
        frame = 'distinct'
    elif aggr_type == 'count' and any('filter' in word for word in operations):
        frame = 'count'
    else:
        frame = 'aggregate'

    if frame == 'count':
        filter_attr = operations['filter_attr'] 
        filter_value = operations['filter_value']
        condition = f'{{"{filter_attr}" : "{filter_value}"}}'
        query = nosql_functions_frames[frame].format(table=table.table_name, conditions=condition)
    
    elif frame == 'distinct':
        query = nosql_functions_frames[frame].format(table=table.table_name, attr=operations['grouped_by'])

    elif frame == 'find':
        filter_attr = operations['filter_attr'] 
        filter_value = operations['filter_value']

        if filter_attr in numeric_columns:
            filter_type = operations['filter_type']
            operator = comp_operators[filter_type]
            filter_value = f'{{"{operator}" : {filter_value}}}'
            condition = f'{{"{filter_attr}" : {filter_value}}}'
        else:
            condition = f'{{"{filter_attr}" : "{filter_value}"}}'

        query = nosql_functions_frames[frame].format(table=table.table_name, conditions=condition)

        if 'sort_by' in operations:
            sort_by = operations['sort_by']
            sort_order = operations['sort_order']
            sort_condition = f'{{"{sort_by}": {sort_order}}}'
            sort_query = find_sort.format(condition=sort_condition)
            query += sort_query

        if 'skip' in operations:
            num = operations['skip']
            skip_query = find_skip.format(num=num)
            query += skip_query

        if 'limit' in operations:
            num = operations['limit']
            limit_query = find_limit.format(num=num)
            query += limit_query

    else:
        grouped_by = operations['grouped_by']
        
        # count operation does not need to be format
        if aggr_type == 'count':
            aggr_attr = grouped_by
            operator = 'count'
        else:
            aggr_attr = operations['aggr_attr']
            operator = [key for key in ari_operators if aggr_type in key][0]
        # print(aggr_attr)

        query = find_unwind_attrs(column_types, aggr_attr)
        group_query, aggr_title = nosql_format_group_query(operator, aggr_attr, grouped_by)
        query = query + group_query
        
        if 'sort_by' in operations:
            sort_by = operations['sort_by']
            sort_order = operations['sort_order']
            sort_condition = f'{{"{sort_by}": {sort_order}}}'
            sort_query = agg_sort.format(condition=sort_condition)
            query += sort_query

        if 'skip' in operations:
            num = operations['skip']
            skip_query = agg_skip.format(num=num)
            query += skip_query

        if 'limit' in operations:
            num = operations['limit']
            limit_query = agg_limit.format(num=num)
            query += limit_query

        query = nosql_functions_frames[frame].format(table=table.table_name, query=query)
        
    
    return query
