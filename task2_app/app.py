from flask import Flask, request, abort, jsonify
from datetime import datetime
import psycopg2
import pandas as pd
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

# Set database credentials
DB_HOST = 'localhost'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'Password@123'


@app.route('/api/file-import', methods=['POST'])
def upload_csv():
    logging.info('Received request to upload file')
    
    # Validate request parameters
    try:
        validate_request(request)
    except Exception as e:
        logging.error(f'Validation failed with error: {e}')
        return jsonify({"error": str(e)}), 400

    # Get file from request
    file = request.files['files']
    
    # Get create user id from request
    create_usr_id = request.form.get('create_usr_id')

    # Get schema from request
    schema = request.form.get('schema')

    # Generate timestamp for new table
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    # Generate table name
    table_name = f"{schema}.master_study_list_{timestamp}"

    try:
        # Read CSV file data with pandas
        df = pd.read_csv(file)

        # Open connection to database
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

        # Create cursor object
        cur = conn.cursor()

        create_query = f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, topic_name VARCHAR(255))"
        cur.execute(create_query)
        conn.commit()
        logging.info('Table created successfully')

        # Insert data from pandas dataframe into new table
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} (id,topic_name) VALUES (%s,%s)"
            cur.execute(insert_query, (index,row[0]))

        # Commit changes to database
        conn.commit()
        logging.info('Data inserted in table successfully')

        # Close cursor and database connection
        cur.close()
        conn.close()
        
        logging.info('File uploaded and table created successfully')
        return jsonify({"status": 200, "message": "File uploaded and table created successfully."})
    except pd.errors.EmptyDataError as e:
        logging.error(f'Failed to read CSV file with error: {e}')
        return jsonify({"error": "Failed to read CSV file"}), 400
    except Exception as e:
        logging.error(f'Failed to create table or upload data with error: {e}')
        return jsonify({"error": "Failed to create table or upload data"}), 500


def validate_request(request):
    # Get file from request
    file = request.files.get('files')
    if not file:
        raise Exception("No file provided")
    else:
        if file.content_type != 'text/csv':
            raise Exception("Invalid file type. Must be CSV")

    # Get create user id from request
    create_usr_id = request.form.get('create_usr_id')
    if not create_usr_id:
        raise Exception("No create_user_id provided")
        
    if create_usr_id not in ["ashish", "likhith"]:
        raise Exception("Invalid create_usr_id")

    # Get schema from request
    schema = request.form.get('schema')
    if not schema:
        raise Exception("No schema provided")


if __name__ == '__main__':
    app.run(debug=True)
