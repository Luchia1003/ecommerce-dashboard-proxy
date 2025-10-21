#
# app.py (Final Version with Timestamp Fix)
#
import os
import json
import base64
import logging
import datetime
import pandas as pd
from flask import Flask, Response
from flask_cors import CORS
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Setup ---
app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- JSON Serializer for Datetime/Timestamp objects ---
def json_converter(o):
    """
    This function is a custom converter for the json.dumps() method.
    It checks if an object is a datetime or pandas Timestamp and converts it to a
    standard ISO 8601 string format, which is JSON serializable.
    """
    if isinstance(o, (datetime.datetime, datetime.date, pd.Timestamp)):
        return o.isoformat()
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

# --- Snowflake Connection Logic ---
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
            "insecure_mode": True
        }
        
        log_params = {k: v for k, v in conn_params.items()}
        logging.info(f"Connecting to Snowflake with parameters: {log_params}")
        
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
        raise

queries = {
    # ---- keep same (already working) ----
    "productCosts": """
        SELECT sku, description, unit_cost
        FROM SKU_PROFIT_PROJECT.ERD.MASTER_COST_FACT
    """,

    "orders": """
        SELECT order_id, sales_sku, quantity, sales_margin, product_sales,
               gross_sales, date_time, currency_code, marketplace_name
        FROM SKU_PROFIT_PROJECT.ERD.AMAZON_NEW_ORDER_FACT
    """,

    "refunds": """
        SELECT order_id, sales_sku, quantity, refund_margin, product_refund,
               gross_refund, date_time, currency_code, marketplace_name
        FROM SKU_PROFIT_PROJECT.ERD.AMAZON_NEW_REFUND_FACT
    """,

    # ---- product level (already aligned to your CSV headers) ----
    "inventory_product_level_snap": """
        SELECT
            TRY_TO_NUMBER(PRODUCT_ID)                                  AS "PRODUCT_ID",
            SKU::STRING                                                 AS "SKU",
            NAME::STRING                                                AS "NAME",
            COALESCE(TRY_TO_DECIMAL(PRICE), 0)::FLOAT                   AS "PRICE",
            COALESCE(TRY_TO_DECIMAL(COST), 0)::FLOAT                    AS "COST",
            UPC::STRING                                                 AS "UPC",
            ASIN::STRING                                                AS "ASIN",
            COUNTRY::STRING                                             AS "COUNTRY",
            TO_VARCHAR(CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(UPDATED)),
                      'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')                 AS "UPDATED",
            COALESCE(TRY_TO_NUMBER(TOTAL_ON_HAND),       0)             AS "TOTAL_ON_HAND",
            COALESCE(TRY_TO_NUMBER(TOTAL_AVAILABLE),     0)             AS "TOTAL_AVAILABLE",
            COALESCE(TRY_TO_NUMBER(TOTAL_COMMITTED),     0)             AS "TOTAL_COMMITTED",
            COALESCE(TRY_TO_NUMBER(TOTAL_ALLOCATED),     0)             AS "TOTAL_ALLOCATED",
            COALESCE(TRY_TO_NUMBER(TOTAL_UNALLOCATED),   0)             AS "TOTAL_UNALLOCATED",
            COALESCE(TRY_TO_NUMBER(TOTAL_MFG_ORDERED),   0)             AS "TOTAL_MFG_ORDERED",
            COALESCE(TRY_TO_NUMBER(TO_BE_SHIPPED),       0)             AS "TO_BE_SHIPPED",
            COALESCE(TRY_TO_DECIMAL(HEIGHT), 0)::FLOAT                   AS "HEIGHT",
            COALESCE(TRY_TO_DECIMAL(WEIGHT), 0)::FLOAT                   AS "WEIGHT",
            COALESCE(TRY_TO_DECIMAL(WIDTH),  0)::FLOAT                   AS "WIDTH",
            TAGS::STRING                                                 AS "TAGS",
            CARTS::STRING                                                AS "CARTS"
        FROM SKU_PROFIT_PROJECT.ERD.INVENTORY_PRODUCT_LEVEL_SNAP
    """,

    # ---- warehouse level (EXACT order & names you provided) ----
    "inventory_warehouse_level_snap": """
        SELECT
            -- shared columns (exact same names as product CSV)
            TRY_TO_NUMBER(PRODUCT_ID)                                   AS "PRODUCT_ID",
            SKU::STRING                                                 AS "SKU",
            NAME::STRING                                                AS "NAME",
            COALESCE(TRY_TO_DECIMAL(PRICE), 0)::FLOAT                   AS "PRICE",
            COALESCE(TRY_TO_DECIMAL(COST), 0)::FLOAT                    AS "COST",
            UPC::STRING                                                 AS "UPC",
            ASIN::STRING                                                AS "ASIN",
            COUNTRY::STRING                                             AS "COUNTRY",
            TO_VARCHAR(CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(UPDATED)),
                      'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')                 AS "UPDATED",
            COALESCE(TRY_TO_NUMBER(TOTAL_ON_HAND),       0)             AS "TOTAL_ON_HAND",
            COALESCE(TRY_TO_NUMBER(TOTAL_AVAILABLE),     0)             AS "TOTAL_AVAILABLE",
            COALESCE(TRY_TO_NUMBER(TOTAL_COMMITTED),     0)             AS "TOTAL_COMMITTED",
            COALESCE(TRY_TO_NUMBER(TOTAL_ALLOCATED),     0)             AS "TOTAL_ALLOCATED",
            COALESCE(TRY_TO_NUMBER(TOTAL_UNALLOCATED),   0)             AS "TOTAL_UNALLOCATED",
            COALESCE(TRY_TO_NUMBER(TOTAL_MFG_ORDERED),   0)             AS "TOTAL_MFG_ORDERED",
            COALESCE(TRY_TO_NUMBER(TO_BE_SHIPPED),       0)             AS "TO_BE_SHIPPED",
            COALESCE(TRY_TO_DECIMAL(HEIGHT), 0)::FLOAT                   AS "HEIGHT",
            COALESCE(TRY_TO_DECIMAL(WEIGHT), 0)::FLOAT                   AS "WEIGHT",
            COALESCE(TRY_TO_DECIMAL(WIDTH),  0)::FLOAT                   AS "WIDTH",
            TAGS::STRING                                                 AS "TAGS",
            CARTS::STRING                                                AS "CARTS",

            -- warehouse-specific columns (match your exact headers & order)
            TRY_TO_NUMBER(WH_ID)                                        AS "WH_ID",
            WH_NAME::STRING                                             AS "WH_NAME",
            TO_VARCHAR(CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(WH_UPDATED)),
                      'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')                 AS "WH_UPDATED",
            TO_VARCHAR(CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(WH_CREATED)),
                      'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')                 AS "WH_CREATED",
            TO_VARCHAR(CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(WH_LAST_CHANGE)),
                      'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')                 AS "WH_LAST_CHANGE",
            COALESCE(TRY_TO_NUMBER(LOW_STOCK_THTD), 0)                  AS "LOW_STOCK_THTD",
            COALESCE(TRY_TO_NUMBER(OOS_THTD),      0)                   AS "OOS_THTD",
            COALESCE(TRY_TO_NUMBER(POH),           0)                   AS "POH",
            COALESCE(TRY_TO_NUMBER(AOH),           0)                   AS "AOH",
            COALESCE(TRY_TO_NUMBER(CMT),           0)                   AS "CMT",
            COALESCE(TRY_TO_NUMBER(OMO),           0)                   AS "OMO",
            COALESCE(TRY_TO_NUMBER(OPO),           0)                   AS "OPO",
            COALESCE(TRY_TO_NUMBER(ON_HAND),       0)                   AS "ON_HAND",
            COALESCE(TRY_TO_NUMBER(ALLOCATED),     0)                   AS "ALLOCATED",
            COALESCE(TRY_TO_NUMBER(UNALLOCATED),   0)                   AS "UNALLOCATED",
            (WH_SHIP_CFG)::BOOLEAN                                       AS "WH_SHIP_CFG",
            (WH_IS_DEFAULT)::BOOLEAN                                     AS "WH_IS_DEFAULT",
            LOCATION::STRING                                              AS "LOCATION"
        FROM SKU_PROFIT_PROJECT.ERD.INVENTORY_WAREHOUSE_LEVEL_SNAP
    """
}

# --- Data Streaming Logic ---
def stream_data():
    """Generator function that connects to Snowflake and streams data in NDJSON format."""
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        for key, query in queries.items():
            logging.info(f"Executing query for: {key}")
            cursor.execute(query)
            
            for df_chunk in cursor.fetch_pandas_batches():
                records = df_chunk.to_dict('records')
                
                payload = {
                    "type": key,
                    "data": records
                }
                
                # Use the custom converter to handle Timestamp objects
                yield json.dumps(payload, default=json_converter) + '\n'
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
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
