from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.utils import secure_filename
import os
from pymongo import MongoClient
from sqlalchemy import create_engine
import src.backend.setup as setup
from src.backend.helper import *
from src.backend.Database import Database
from src.backend.upload import *
from src.backend.sql.sql_query import *
from src.backend.Table import *
from src.backend.nosql.nosql_analyze_input import *

app = Flask(__name__)
CORS(app)
app.config["UPLOAD_FOLDER"] = "uploads"
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
last_query = []
tables = []  # Store tables globally for simplicity, could be refactored as needed
database = None  # This should be initialized based on your database logic
random_queries = ["queries", "sample", "example"] 
@app.route("/api/upload", methods=["POST"])
def upload_file():
    global database, initial_database_name  # Add global variable for tracking initial database name

    if "file" not in request.files:
        return jsonify({"message": "No file part in the request"}), 400

    file = request.files["file"]
    database_type = request.form.get("databaseType")

    if file.filename == "":
        return jsonify({"message": "No file selected"}), 400

    if file:
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(file_path)
        
        # Use the initial database name or create a new one if it's the first upload
        if "initial_database_name" not in globals() or initial_database_name is None:
            initial_database_name = os.path.splitext(os.path.basename(file_path))[0]

        database_name = initial_database_name
        if(not database):
            database = Database(database_name)

        # Read and process the file
        try:
            result_message = {"database_name": database_name, "tables": {}}
            sample_data = {}
            # Use the appropriate upload function based on selected database type
            if database_type == "SQL":
                database.client = "sql"
                mysql_user = setup.MYSQL_USER
                mysql_pw = setup.MYSQL_PASSWORD
                mysql_host = setup.MYSQL_HOST
                db_url = f"mysql+pymysql://{mysql_user}:{mysql_pw}@{mysql_host}"

                processed_table = upload_dataset_to_rdbms(file_path, db_url, database)

                for table in processed_table:
                     result_message["tables"][table.table_name] = table.generate_table_descriptions()
                     sample_data = fetch_table_preview(db_url, database, table)

            elif database_type == "NoSQL":
                database.client = "nosql"
                db_url = f"mongodb://localhost:27017/{database_name}"
                processed_table = upload_dataset_to_nosql(file_path, db_url, database)
                for table in processed_table:
                    result_message["tables"][table.table_name] = table.generate_table_descriptions()
                    sample_data= get_nosql_sample_data(database, table)
            else:
                result_message = {"database_name": database_name, "error": "Invalid database type."}
        except Exception as e:
            print("Exception occurred:", e)  # Log the exception
            return jsonify({"message": f"File upload failed due to: {str(e)}"}), 500
        finally:
            os.remove(file_path)

        return jsonify({
                "message": "File uploaded successfully!",
                "result": result_message,
                "sample_data": sample_data,
                "prompt": "Let me know if you need further analysis or insights from this data. Type 'quit' to exit."
            }), 200

@app.route("/api/process_input", methods=["POST"])
def process_input():
    global database, initial_database_name,last_query
    data = request.json
    user_input = data.get("user_input", "").lower()
    if any(word in user_input.split() for word in exit_words): 
        if (database and database.client == "sql"):
            mysql_user = setup.MYSQL_USER
            mysql_pw = setup.MYSQL_PASSWORD
            mysql_host = setup.MYSQL_HOST
            db_url = f"mysql+pymysql://{mysql_user}:{mysql_pw}@{mysql_host}"
            engine = create_engine(db_url)
            with engine.connect() as conn:
                conn.execute(text(f"DROP DATABASE IF EXISTS {initial_database_name}"))
        elif(database and database.client == "nosql"):
            db_url = f"mongodb://localhost:27017/{initial_database_name}"
            client = MongoClient(db_url)
            client.drop_database(initial_database_name)
        database = None
        initial_database_name = None 
        return jsonify({
            "message": "Session ended. You can start a new session by uploading a file.",
        }), 200
    # Check if the user input matches random_queries
    elif any(word in user_input.split() for word in execute_words):
        idx = 0
        for word in user_input.split():
            if word in valid_values:  # Check if word is in valid_values
                idx = map_to_integer(word) - 1 # Map to integer
                break  # Stop once a match is found
        if (last_query and database and database.client == "sql"):
            query = last_query[idx]
            mysql_user = setup.MYSQL_USER
            mysql_pw = setup.MYSQL_PASSWORD
            mysql_host = setup.MYSQL_HOST
            db_url = f"mysql+pymysql://{mysql_user}:{mysql_pw}@{mysql_host}"
            result = execute_sql_query(db_url, database, query)
            return jsonify({
                "message": "Query executed: ",
                "queries": result,
            }), 200
        if (last_query and database and database.client == "nosql"):
            query = last_query[idx]
            db_url = f"mongodb://localhost:27017/{initial_database_name}"
            database_name = database.name
            result = execute_nosql_query(db_url, database_name, query)
            return jsonify({
                "message": "Query execution result: ",
                "queries": result,
            }), 200
    else:
        if database and database.client == "nosql":
            # Generate NoSQL sample queries
            sample_queries = nosql_analyze_user_input(user_input, database)
            formatted_captions = [f"{q['caption']}\n" for q in sample_queries]
            formatted_queries = [f"{q['query']}" for q in sample_queries]
            last_query = []
            last_query.extend([q['query'] for q in sample_queries])
            return jsonify({"message": "Sample NoSQL queries generated:","captions": formatted_captions,"queries": formatted_queries}), 200
        elif database and database.client == "sql":
            # Generate SQL sample queries
            queries, captions = process_sql_query(user_input, database)
            caption = [f"{captions[i]}\n" for i in range(len(captions))]
            query = [f"{queries[i]}" for i in range(len(queries))]
            if len(captions) < len(queries):
                caption.extend(["None\n"] * (len(queries) - len(captions)))
            last_query = []
            last_query.extend(queries)
            return jsonify({"message": f"Sample SQL query generated:", "captions": caption, "queries": query}), 200
        else:
            return jsonify({"message": "No database connected to generate queries."}), 400


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)

