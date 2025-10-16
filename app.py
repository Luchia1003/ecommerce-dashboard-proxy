#
# app.py (Final Merged & Corrected Version)
#
import os
import json
import base64
import logging
from flask import Flask, Response
from flask_cors import CORS
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Setup ---
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Snowflake Connection Logic (from your working old file) ---
def get_snowflake_connection():
    """Establishes a connection to Snowflake using environment variables."""
    try:
        logging.info("Attempting to decode private key from environment variable...")
        
        private_key_b64 = os.environ.get('PRIVATE_KEY_STR')
        if not private_key_b64:
            raise ValueError("Environment variable PRIVATE_KEY_STR is not set.")
            
        private_key_bytes = base64.b64decode(private_key_b64)
        logging.info("Private key successfully decoded from Base64.")

        p_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=None, 
            backend=default_backend()
        )
        logging.info("Private key object successfully loaded.")

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        logging.info("Private key converted to DER format for connection.")

        # Using the exact environment variable names from your working file
        conn_params = {
            "user": os.environ.get('SNOWFLAKE_USERNAME'),
            "account": os.environ.get('SNOWFLAKE_ACCOUNT'),
            "warehouse": os.environ.get('SNOWFLAKE_WAREHOUSE'),
            "database": os.environ.get('SNOWFLAKE_DATABASE'),
            "schema": os.environ.get('SNOWFLAKE_SCHEMA'),
            "role": os.environ.get('SNOWFLAKE_ROLE'),
            "insecure_mode": True  # Diagnostic flag from your old file
        }
        
        # Log parameters for debugging, but omit the private key itself
        log_params = {k: v for k, v in conn_params.items() if k != 'private_key'}
        logging.info(f"Connecting to Snowflake with parameters: {log_params}")
        
        # Check for empty essential parameters before connecting
        if not all([conn_params['user'], conn_params['account']]):
            raise ValueError("SNOWFLAKE_USERNAME or SNOWFLAKE_ACCOUNT environment variable is empty.")

        conn = snowflake.connector.connect(
            **conn_params,
            private_key=pkb,
        )
        logging.info("<<< SUCCESS! >>> Successfully connected to Snowflake!")
        return conn

    except Exception as e:
        logging.critical(f"An unexpected error occurred in get_snowflake_connection: {e}")
        # Re-raise the exception to be caught by the streaming function
        raise

# --- Queries Definition ---
queries = {
    "productCosts": "SELECT sku, description, unit_cost FROM SKU_PROFIT_PROJECT.ERD.MASTER_COST_FACT",
    "orders": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.AMAZON_NEW_ORDER_FACT",
    "refunds": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.AMAZON_NEW_REFUND_FACT",
    "shipping": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.NEW_SHIPPING",
    "inventory_product_level_snap": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.INVENTORY_PRODUCT_LEVEL_SNAP",
    "inventory_warehouse_level_snap": "SELECT * FROM SKU_PROFIT_PROJECT.ERD.INVENTORY_WAREHOUSE_LEVEL_SNAP"
}

# --- Data Streaming Logic (from your new file) ---
def stream_data():
    """Generator function that connects to Snowflake and streams data in NDJSON format."""
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        for key, query in queries.items():
            logging.info(f"Executing query for: {key}")
            cursor.execute(query)
            
            # Fetch results in chunks (batches) of pandas DataFrames to keep memory low
            for df_chunk in cursor.fetch_pandas_batches():
                records = df_chunk.to_dict('records')
                
                payload = {
                    "type": key,
                    "data": records
                }
                
                # Yield the payload as a JSON string, followed by a newline (NDJSON format)
                yield json.dumps(payload) + '\n'
            logging.info(f"Finished streaming for: {key}")

    except Exception as e:
        logging.error(f"!!! ERROR !!! An error occurred during streaming: {e}")
        error_payload = {
            "type": "error",
            "message": str(e)
        }
        yield json.dumps(error_payload) + '\n'
    finally:
        if conn and not conn.is_closed():
            conn.close()
            logging.info("Snowflake connection closed.")

# --- API Endpoint ---
@app.route('/api/data')
def api_data():
    """API endpoint that returns the streaming data response."""
    return Response(stream_data(), mimetype='application/x-ndjson')

@app.route('/')
def index():
    """Health check route."""
    return "Proxy server is running. Access data at /api/data"

# --- Main Execution ---
if __name__ == '__main__':
    # This block is for local development. Render uses Gunicorn to run the app.
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
