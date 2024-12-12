from src.backend.nosql.nosql_query_templates import *
from src.backend.nosql.nosql_query_helper import *
from src.backend.nosql.generate_nosql_queries import *
from src.backend.nosql.nosql_nlp_query import *

def exclude_pronouns_spacy(text):
    quantifiers = {"some", "many", "few", "all", "several", "any", "most", "each", "every"}
    
    doc = nlp(text)
    
    # Filter tokens
    filtered_words = [
        token.text
        for token in doc
        if token.pos_ not in ["DET", "PRON", "CCONJ", "SCONJ"] or token.text.lower() in quantifiers
    ]
    
    return filtered_words


def nosql_process_nlp_input(database, user_input):
    table = database.tables[0]

    # find the types of all attributes
    column_types = find_nested_column_types(table)

    # list of all attribute names
    attri_list = list(column_types.keys())
    attri_token_list = tokenize_phrase(attri_list)

    # if the type of the attribute is a list or dict, remove the column 
    # and root name from all its children from token list
    temp_all_attrs = []
    temp_all_attrs_token = []

    remove_word = ''
    for i in range(len(attri_list)):
        if column_types[attri_list[i]] in ['list', 'dict']:
            remove_word = attri_list[i]
        else:
            attri_token_list[i] = [word for word in attri_token_list[i] if word not in remove_word]

            temp_all_attrs.append(attri_list[i])
            temp_all_attrs_token.append(attri_token_list[i])

    attri_list = temp_all_attrs
    attri_token_list = temp_all_attrs_token

    # exclude stopwords from user input
    input_split = exclude_pronouns_spacy(user_input) 

    keywords = []
    possible_attrs = []
    possible_attrs_token = []

    # extract words from user input if it is in one of the column names to be keywords
    for token in input_split:
        singular_token = to_singular(token)
        for i in range(len(attri_list)):
            if singular_token in attri_token_list[i]:
                keywords.append(token)
                possible_attrs_token.append(attri_token_list[i])
                possible_attrs.append(attri_list[i])
                break


    input_partition = []
    temp = []
    # Partition user input into trunks that each trunk contains a keyword
    # meaning that 5 keywords will separate input to 5 trunks
    for word in input_split:
        if word in keywords:
            temp.append(word)
            input_partition.append(' '.join(temp))
            temp = []
        else: 
            temp.append(word)
    if temp:
        if not any(word in temp for word in keywords):
            temp = input_partition[-1] + ' ' + ' '.join(temp)
            input_partition.pop()
            input_partition.append(temp)
        else:
            input_partition.append(' '.join(temp))


    check_obj = ''
    temp_keywords = []
    temp_attr_token = []
    temp_attrs = []
    temp_input_partitions = []


    # remove duplicate attribute and attribute that is not important
    # e.g. 'transaction' and 'quantity' are coherent and  both filtered as keywords, 
    # so transaction_id and transaction_quantity will both be selected
    # transaction_quantity contains both keywords, transaction_id is redundant
    # so we reversely loop the the selected attributes to exclude transaction_id if it is not used in sort by
    for j in range(len(possible_attrs)-1, -1, -1):
        same_obj = possible_attrs_token[j][0]
        
        if 'sort' in input_partition[j]: 
            if check_obj == same_obj:
                comb_partition = input_partition[j] + ' ' + temp_input_partitions[0]
                temp_input_partitions.pop(0)
                temp_input_partitions.insert(0, comb_partition)
                continue

            temp_keywords.insert(0, keywords[j])
            temp_attr_token.insert(0, possible_attrs_token[j])
            temp_attrs.insert(0, possible_attrs[j])
            temp_input_partitions.insert(0, input_partition[j])

        elif check_obj != same_obj:
            temp_keywords.insert(0, keywords[j])  
            temp_attr_token.insert(0, possible_attrs_token[j])
            temp_attrs.insert(0, possible_attrs[j])
            temp_input_partitions.insert(0, input_partition[j])
        
            check_obj = same_obj

        elif check_obj == same_obj:
            comb_partition = input_partition[j] + ' ' + temp_input_partitions[0]
            temp_input_partitions.pop(0)
            temp_input_partitions.insert(0, comb_partition)

    keywords = temp_keywords
    possible_attrs = temp_attrs
    possible_attrs_token = temp_attr_token
    input_partition = temp_input_partitions

    operations = nosql_attrs_function_match(column_types, possible_attrs, possible_attrs_token, input_partition)

    return operations


# the function to analyze the user input and determine which operation needs to be done
# parameters:
# user_input: input string
# database: Database obejct
# return: list of generated queries
def nosql_analyze_user_input(user_input, database):

    # asking for sample queries 
    if any(word in user_input.split() for word in sample_query_words):

        l = len(database.tables)

        user_input_token = user_input.split(' ')
        if_function = [word for word in nosql_functions if word in user_input_token]
        if_keyword = [word for word in addition_keywords if word in user_input_token]

        # find the list of templates that can be selected
        available_templates = []
        required_operators = [] 

        if len(if_function) != 0 and len(if_keyword) != 0:
            for spec_function in if_function:
                available_templates += templates_set[spec_function]

                if spec_function == 'aggregate' and l > 1:
                    available_templates += templates_set['lookup']

            required_operators = [tokenize_phrase([word])[0][0] for word in if_keyword]

        elif if_keyword:
            if any('lookup' in word or 'match' in word for word in if_keyword):
                available_templates = templates_set['lookup']
            elif any('$' in word for word in if_keyword):
                available_templates += templates_set['aggregate']
                if spec_function == 'aggregate' and l > 1:
                    available_templates += templates_set['lookup']
            else:
                available_templates = available_templates + templates_set['aggregate'] + templates_set['find']

            required_operators = [tokenize_phrase([word])[0][0] for word in if_keyword]

        elif if_function:
            for keyword in if_function:
                available_templates += templates_set[keyword]

                # if there are multiple tables in the database, add the templates include $lookup
                if keyword == 'aggregate' and l > 1:
                    available_templates += templates_set['lookup']

        elif any(word in user_input for word in sample_query_words):
            for key, value in templates_set.items():
                if key == 'lookup' and l <= 1:
                    continue

                available_templates += value             

        sample_query_list = get_nosql_sample_queries(database, available_templates, required_operators)

    # asking question in natural language
    else:
        operations = nosql_process_nlp_input(database, user_input)
        query = nosql_format_nlquery(database, operations)

        sample_query = {
            'num': 1, 
            'template': 'nl', 
            'caption': None, 
            'query': query}
        
        sample_query_list = [sample_query]

    return sample_query_list