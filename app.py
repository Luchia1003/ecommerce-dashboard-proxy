#
# app.py (New Streaming Version)
#
import os
import json
import base64
from flask import Flask, Response, jsonify
from flask_cors import CORS
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization

app = Flask(__name__)
CORS(app)

# All connection details from Render Environment Variables
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA')
PRIVATE_KEY_STR_BASE64 = os.environ.get('PRIVATE_KEY_STR')

# Decode the Base64 private key
try:
    p_key_bytes = base64.b64decode(PRIVATE_KEY_STR_BASE64)
    private_key = serialization.load_pem_private_key(
        p_key_bytes,
        password=None,
    )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    print(">>> SUCCESS! >>> Private key decoded successfully.")
except Exception as e:
    pkb = None
    print(f"!!! CRITICAL ERROR !!! Failed to decode private key: {e}")


# Define all queries in one place
queries = {
    "productCosts": "SELECT sku, description, unit_cost FROM SKU_PROFIT_PROJECT.ERD.MASTER_COST_FACT",
    "orders": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.AMAZON_NEW_ORDER_FACT",
    "refunds": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.AMAZON_NEW_REFUND_FACT",
    "shipping": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.NEW_SHIPPING",
    "inventory_product_level_snap": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.INVENTORY_PRODUCT_LEVEL_SNAP",
    "inventory_warehouse_level_snap": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.INVENTORY_WAREHOUSE_LEVEL_SNAP"
}

def get_snowflake_connection():
    if not pkb:
        raise ConnectionError("Snowflake private key is not valid. Cannot connect.")
    
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        private_key=pkb,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    print(">>> SUCCESS! >>> Successfully connected to Snowflake!")
    return conn

def stream_data():
    """This is a generator function that streams data in chunks."""
    conn = None
    try:
        conn = get_snowflake_connection()
        with conn.cursor() as cursor:
            for key, query in queries.items():
                print(f"Executing query for: {key}")
                cursor.execute(query)
                
                # Fetch results in chunks (batches) of pandas DataFrames
                for df_chunk in cursor.fetch_pandas_batches():
                    # Convert DataFrame chunk to a list of dictionaries
                    records = df_chunk.to_dict('records')
                    
                    # Create the payload for this chunk
                    payload = {
                        "type": key,
                        "data": records
                    }
                    
                    # Yield the payload as a JSON string, followed by a newline
                    # This is the NDJSON (Newline Delimited JSON) format
                    yield json.dumps(payload) + '\n'
                print(f"Finished streaming for: {key}")

    except Exception as e:
        print(f"!!! ERROR !!! An error occurred during streaming: {e}")
        error_payload = {
            "type": "error",
            "message": str(e)
        }
        yield json.dumps(error_payload) + '\n'
    finally:
        if conn and not conn.is_closed():
            conn.close()
            print("Snowflake connection closed.")

@app.route('/api/data')
def api_data():
    # Return a streaming response. The web server (Gunicorn) will handle iterating
    # through the generator and sending chunks to the client.
    return Response(stream_data(), mimetype='application/x-ndjson')

if __name__ == '__main__':
    app.run(debug=True)
