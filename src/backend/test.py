import os
import sys
from src.backend.setup import *
from src.backend.upload import *
from src.backend.predefined_list import *
from src.backend.Database import Database
from src.backend.helper import *
from src.backend.nosql.nosql_analyze_input import *
from src.backend.sql.sql_query import *

# Main execution logic
if __name__ == "__main__":

    mysql_user = MYSQL_USER
    mysql_pw = MYSQL_PASSWORD
    mysql_host = MYSQL_HOST

    db_urls = f"mysql+pymysql://{mysql_user}:{mysql_pw}@{mysql_host}"
    db_urlm = 'mongodb://localhost:27017/'

    if len(sys.argv) < 3:
        print("Invalid command. Usage: python script.py [SQL/NoSQL] [file_name]")
        sys.exit(1)

    file_path = sys.argv[2]
    database_name = os.path.splitext(os.path.basename(file_path))[0]
    database = Database(database_name)

    if sys.argv[1] == 'SQL':
        database.set_client('sql')
        new_tables = upload_dataset_to_rdbms(file_path, db_urls, database)
    elif sys.argv[1] == 'NoSQL':
        database.set_client('nosql')
        new_tables = upload_dataset_to_nosql(file_path, db_urlm, database)

    # print table descriptions in each table
    for table in new_tables:
        table.print_table_descriptions()

    user_input = input('\nLet me know if you need further analysis or insights from this data. Type "quit" to exit.\n')

    if_upload = any(word in user_input for word in acceptable_file_types)

    while if_upload:
        if database.client == 'sql':
            new_tables = upload_dataset_to_rdbms(user_input, db_urls, database)
        elif database.client == 'nosql':
            new_tables = upload_dataset_to_nosql(user_input, db_urlm, database)
    
        for table in new_tables:
            table.print_table_descriptions()

        user_input = input('\nLet me know if you need further analysis or insights from this data. Type "quit" to exit.\n')

        if_upload = any(word in user_input for word in acceptable_file_types)

    user_inputs.append(user_input)

    # if the user wants to exit
    if_exit = any(word in user_input.lower().split() for word in exit_words)
    if if_exit:
        sys.exit(1)
    else: 
        # if the user chose NoSQL database to upload to
        if(database.client == 'nosql'):
            if 'sample data' in user_input:
                for table in new_tables:
                    print(get_nosql_sample_data(database, table))
            else: 
                sample_queries = nosql_analyze_user_input(user_input, database)
                print_nosql_queries(sample_queries)
        elif (database.client == 'sql'):
            chosen_table = random.choice(database.tables)
            table_name = chosen_table.table_name
            table = chosen_table.df
            columns = chosen_table.extract_columns()
            datatype = chosen_table.set_column_types()
            queries, captions = generate_sample_queries(table_name, columns, datatype)
            print_queries(queries, captions)