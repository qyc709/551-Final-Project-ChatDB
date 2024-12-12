
class Database:
    def __init__(self, name):
        self.name = name
        self.tables = []

    # set client to sql or nosql
    def set_client(self, client):
        self.client = client
        return

    def create_tables(self, new_tables):
        for new_table in new_tables:
            self.tables.append(new_table)
        return