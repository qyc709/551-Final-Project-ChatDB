# ChatDB

### Running the project

+ Install all required packages in *requirements.txt* and start local MySQL server

+ **Modify  `src/backend/setup.py`**: fill in mysql server password to setup sql database configuration.

+ **Start a new terminal session**:
  - Navigate to the correct directory: `cd src/frontend`
  - Run: `npm start`

+ **Start another new terminal session**:
  - Stay at the **project root** and run: `python -m src.main`.



### Restrictions

+ ChatDB only accepts one database per conversation session. If need to upload to new database, remember to enter **'quit'** to end the current session to clear previous database.
+ MongoDB, documents in a collection should have the **same structure**.
+ Natural language input only accepts querying **single** table/collection, no join is allowed.
+ Support SQL constructs to specify
  + `SELECT` `FROM` `WHERE` `GROUP BY` `ORDER BY` `LIMIT`
+ Support NoSQL constructs to specify
  + `find` `aggregate` `count` `distinct` `sort` `limit` `skip` `match` `lookup`
  + comparison and aggregation operators can only show in random sample queries with probability



### File structure

+ ***requirements.txt***:  listing all required packages to run the project
+ **data**: all testing datasets used to test the current version ChatDB and referencing data file
+ **src**: all source codes
  + ***main.py***: main file of the project, run this file to start ChatDB. Receiving messages from frontend and call relative functions in backend
  + ***package.json*** and ***package-lock.json***: configuration file to run frontend webpage
  + **node_modules**: required modules to run frontend webpage
  + **backend**: backend implementatin code
    + ***Database.py***: `Database` class definition
    + ***Table.py***: `Table` class definition
    + ***setup.py***: MySQL server configuration setup
    + ***predefined_list.py***: pre-defined keywords used for inital user input analysis. Checking one of the following situations:
      1. end the current session
      2. file upload
      3. request for sample query
      4. query execution
    + ***server.js***: connection between frontend and backend
    + ***helper.py***: helper functions to analyzer user inputs and convert query to human readable text
    + ***test.py***: main file to test the backend implementation, no frontend connection
    + **nosql**: implementation code to generate NoSQL queries
      + ***nosql_query_templates.py***: nosql query templates and keywords definition
      + ***nosql_analyze_input.py***: functions to analyze user inputs if users request for nosql queries
      + ***generate_nosql_queries.py***: functions to generate nosql random sample queries and queries with specific constructs.
      + ***nosql_nlp_query.py***: functions mapping natural language inputs to components of nosql query
      + ***nosql_query_helper.py***: helper functions to dig into nosql data, such as fetch value from MongoDB, check valid join of two collections etc..
    + **sql**: implementation code to generate SQL queries
      + ***sql_query.py***: all implementation of generating sql queries. Including sql query templates, getting sample data from MySQL, generating random sql sample queries, queries with specific constructs, and natural language query requests.
  + **frontend**: frontend implementation code
    + ***package.json*** and ***package-lock.json***: configuration file to run frontend webpage
    + **public**: elements of webpage
    + **src**: all source codes of frontend implementation
      + ***App.css***: UI setup
      + ***firebase.js***: firebase congifuration, place to store all messages
      + ***App.js***: main file to implement user interaction and response. Including file upload, message detection, etc..