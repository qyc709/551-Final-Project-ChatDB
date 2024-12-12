from src.backend.helper import *

class Table:
    def __init__(self, table_name, df):
        self.table_name = table_name
        self.df = df

        self.primary_key = self.set_primary_key()
        # print("Primary key:", self.primary_key, '\n')

        self.column_types = self.set_column_types()

    def extract_columns(self):
        return self.df.columns.tolist()
    
    
    def set_column_types(self):
        data = self.df

        column_types = {}
        for column in data:
            column_types[column] = type(data[column][0]).__name__

        return column_types

    
    def set_primary_key(self):
        data = self.df

        # find columns that do not have duplicate values, each value in the column is unique
        unique_columns = []
        for col in data.columns:
            list_column = data[col].apply(lambda x: isinstance(x, (list, dict))).any()
            if not list_column:
                if data[col].is_unique:
                    unique_columns.append(col)

        # tokenize the unique column names to see if there is an 'id' column, if yes, set as primary key
        # if no, pick the first unique column as the primary key
        unique_columns_token = tokenize_phrase(unique_columns)

        # No primary key
        if len(unique_columns_token) == 0:
            return None
        
        # Prefer to select the first column with 'id'
        id_columns = [i for i, col in enumerate(unique_columns_token) if 'id' in col]

        if len(id_columns) != 0:
            primary_key = unique_columns[id_columns[0]]
        else:
            primary_key = unique_columns[0]

        return primary_key


    def generate_table_descriptions(self):
        column_names = self.extract_columns()

        column_name_split = tokenize_phrase(column_names)

        table_descriptions = {}

        # pre-defined format to output the description of columns
        for i in range(len(column_names)):

            obj = get_temp_key(self.table_name, column_names[i])

            if column_names[i] == self.primary_key:
                table_descriptions[column_names[i]] = f"Unique identifier for each {to_singular(obj)}"
            elif to_singular(column_names[i]) == to_singular(self.table_name):
                table_descriptions[column_names[i]] = f'{column_names[i]}'
            elif 'id' in column_name_split[i]:
                table_descriptions[column_names[i]] = f"Identifier for the {to_singular(obj)}"
                obj = column_name_split[i][0]
            elif 'unit' in column_name_split[i]:
                index = [j for j in range(len(column_name_split[i])) if column_name_split[i][j] == 'unit']
                index = index[0]

                table_descriptions[column_names[i]] = f"{column_name_split[i][index+1].capitalize()} per unit of the {column_name_split[i-1][0]}"
            elif 'quantity' in column_name_split[i]:
                table_descriptions[column_names[i]] = "Quantity of the items"
            elif 'detail' in column_names[i] or 'description' in column_names[i]:
                table_descriptions[column_names[i]] = f"Detailed description of the {to_singular(obj)}"
            elif 'creat' in column_name_split[i] and 'at' in column_name_split[i]:
                table_descriptions[column_names[i]] = "Creat time of the record"
            elif 'created' in column_name_split[i] and 'at' in column_name_split[i]:
                table_descriptions[column_names[i]] = "Creat time of the record"
            elif 'password' in column_name_split[i]:
                table_descriptions[column_names[i]] = "Password of the account"
            else:
                if obj == column_name_split[i][0]:
                    concat = " ".join(column_name_split[i][1:])
                else:
                    concat = " ".join(column_name_split[i])

                table_descriptions[column_names[i]] = f"{concat.capitalize()} of the {to_singular(obj)}"

        return table_descriptions
    

    def print_table_descriptions(self):

        table_descriptions = self.generate_table_descriptions()

        print(self.table_name)

        for key, value in table_descriptions.items():
            print(f"{key}: {value}")

        print('\n')
        return