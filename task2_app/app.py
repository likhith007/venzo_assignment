from flask import Flask, request
from datetime import datetime
import psycopg2
import pandas as pd

app = Flask(__name__)

# Set database credentials
DB_HOST = 'localhost'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'Password@123'


@app.route('/api/file-import', methods=['POST'])
def upload_csv():
    # Get file from request
    file = request.files['files']
    print(file)
    print(request.form)
    # Get create user id from request
    create_usr_id = request.form.get('create_usr_id')

    if create_usr_id not in ["ashish", "likhith"]:
        return {"status": 404, "message": "User not found" }

    # Get schema from request
    schema = request.form.get('schema')

    # Generate timestamp for new table
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    # Generate table name
    table_name = f"{schema}.master_study_list_{timestamp}"

    # Read CSV file data with pandas
    df = pd.read_csv(file)

    # Open connection to database
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

    # Create cursor object
    cur = conn.cursor()
   
    create_query = f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, topic_name VARCHAR(255))"
    cur.execute(create_query)
    conn.commit()
    
    # Insert data from pandas dataframe into new table
    for index, row in df.iterrows():
        print(type(row))
        print(row)
        insert_query = f"INSERT INTO {table_name} (id,topic_name) VALUES (%s,%s)"
        cur.execute(insert_query, (index,row[0]))

    # Commit changes to database
    conn.commit()

    # Close cursor and database connection
    cur.close()
    conn.close()

    # Return success message
    return {"status": 200,
            "message": "File uploaded and table created successfully."}


if __name__ == '__main__':
    app.run(debug=True)