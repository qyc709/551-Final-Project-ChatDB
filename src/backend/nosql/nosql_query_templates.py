nosql_query_templates = [
    {'num': 1,
     'query_type': 'categorical',
     'syntax': 'db.{table}.find({{ "{column}": {value} }})', 
     'caption': "Find all {key} of a specific {column}"},

    {'num': 2, 
     'query_type': 'numeric',
     'syntax': 'db.{table}.find({{ "{column}": {{ "{operator}": {value} }}}})',
     'caption': 'Find all {key} with {column} {operator} {value}'},

    {'num': 3, 
     'query_type': 'categorical',
     'syntax': 'db.{table}.countDocuments({{ "{column}": {value} }})',
     'caption': 'Find the total number of {key} for a specific {column}'},

    {'num': 4, 
     'query_type': 'numeric',
     'syntax': 'db.{table}.countDocuments({{ "{column}": {{ "{operator}": {value} }}}})',
     'caption': 'Count the number of {key} with {column} {operator} {value}'},

    {'num': 5, 
     'query_type': 'categorical',
     'syntax': 'db.{table}.distinct("{column}")',
     'caption': 'List all unique {column} available in the dataset'},

    {'num' : 6,
     'query_type': 'aggregate else',
     'syntax': 'db.{table}.aggregate({query})',
     'caption': 'Calculate {operator} {aggr_field} grouped by {column}'
    },

    {'num' : 7 ,
     'query_type': 'aggregate count',
     'syntax': 'db.{table}.aggregate({query})',
     'caption': '{operator} the number of different {aggr_field} grouped by {column}'
    },

    {'num': 8,
     'query_type': 'lookup',
     'syntax': 'db.{table}.aggregate({query})', 
     'caption': 'Find all {key} with {column} {operator} {value}'
    }
]

templates_set = {
    'find': [1, 2],
    'count': [3, 4],
    'distinct': [5],
    'aggregate': [6, 7],
    'lookup': [8]
}

nosql_functions = list(templates_set.keys())

nosql_functions_frames = {
    'find': 'db.{table}.find({conditions})',
    'count': 'db.{table}.countDocuments({conditions})',
    'distinct': 'db.{table}.distinct("{attr}")',
    'aggregate': 'db.{table}.aggregate({query})'
}


addition_keywords = ['sort', 'skip', 'limit', '$sort', '$skip', '$limit', 'lookup', '$lookup', 'match', '$match']

comp_operators = { 
    'greater than': '$gt',
    'greater than or equal to': '$gte',
    'equal to': '$eq',
    'less than': '$lt',
    'less than or equal to': '$lte',
    'not equal to': '$ne',
    'in': '$in'
}

ari_operators = {
    'sum of' : '{{$sum: "${attr}"}}',
    'count': '{$sum: 1}',
    'average' : '{{$avg: "${attr}"}}',
    'highest' : '{{$max: "${attr}"}}',
    'least' : '{{$min: "${attr}"}}'
}

unwind = '{{$unwind: "${unwind_attr}"}}'
group = '{{$group: {group_query}}}'
lookup = '{{$lookup: {{from: "{foreign}", localField:"{local}", foreignField:"{foreign_key}", as: "tmp"}}}}'
match = '{{$match: {conditions}}}'
project = ', {{$project: {{"_id": 0, "{ccolumn}": "$_id", {projection}}}}}'

agg_sort = ', {{$sort:{condition}}}'
agg_limit = ', {{$limit: {num}}}'
agg_skip = ', {{$skip: {num}}}'

find_sort = '.sort({condition})'
find_skip = '.skip({num})'
find_limit = '.limit({num})'

aggr_types = {
    'sum' : ['total', 'sum', 'sum of', 'most sold', 'least sold'],
    'average' : ['average', 'avg', 'mean', 'per unit'],
    'count' : ['count', 'count of', 'number of'],
    'distinct': ['distinct'],
    'no_aggregate': ['all']
}

if_group = ['by', 'per', 'for each', 'on', 'based on', 'each']

if_sort = {
    '1': ['asc', 'ascend', 'ascending', 'increase', 'increas', 'sort'],
    '-1': ['desc', 'descending', 'decreas', 'decreasing']
}

if_filter = {
    'greater than': ['above', 'greater than', '>'],
    'greater than or equal to': ['greater than or equal to', '>='],
    'equal to': ['equal', '='],
    'less than': ['below', 'lower than', '<'],
    'less than or equal to': ['lower than or equal to', '<='],
    'not equal to': ['not equal to', 'is not', '!='],
    'filter': ['e.g.', 'where', 'with', 'for specific']
}

if_limit = {
    '1' : ['least', 'lowest', 'smallest'], # sort: 1, limit: 1
    '-1': ['most', 'highest', 'largest'], # sort: -1, limit: 1
    'top' : ['top'], # sort: -1, limit: n
    'limit' : ['limit'] #limit: 
}

if_skip = ['skip']