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

# --- Queries Definition ---
# --- Queries Definition ---
# 请用下面的整个字典替换您文件中的同名部分
queries = {
    # 这个查询保持不变，因为它已经明确指定了列
    "productCosts": """
        SELECT sku, description, unit_cost
        FROM SKU_PROFIT_PROJECT.ERD.MASTER_COST_FACT
    """,
    
    # 这个查询从您的旧版本中恢复，以避免使用不安全的 `SELECT *`
    "orders": """
        SELECT order_id, sales_sku, quantity, sales_margin, product_sales,
               gross_sales, date_time, currency_code, marketplace_name
        FROM SKU_PROFIT_PROJECT.ERD.AMAZON_NEW_ORDER_FACT
    """,

    # 这个查询也从旧版本中恢复
    "refunds": """
        SELECT order_id, sales_sku, quantity, refund_margin, product_refund,
               gross_refund, date_time, currency_code, marketplace_name
        FROM SKU_PROFIT_PROJECT.ERD.AMAZON_NEW_REFUND_FACT
    """,

    # 这个查询同样从旧版本中恢复
    "shipping": """
        SELECT ship_date, order_id, items, shipping_cost
        FROM SKU_PROFIT_PROJECT.ERD.NEW_SHIPPING
    """,

    # --- 关键修复 ---
    # 这里所有的 ::DATATYPE 都被替换为了 TRY_CAST(COLUMN AS DATATYPE)。
    # 这将安全地处理数据格式问题，遇到无法转换的值时会返回 NULL，而不是让整个查询失败。
    "inventory_product_level_snap": """
        SELECT
            TRY_CAST(ASIN AS VARCHAR) AS ASIN,
            TRY_CAST(CARTS AS VARCHAR) AS CARTS,
            TRY_CAST(COST AS FLOAT) AS COST,
            TRY_CAST(COUNTRY AS VARCHAR) AS COUNTRY,
            TRY_CAST(HEIGHT AS FLOAT) AS HEIGHT,
            TRY_CAST(NAME AS VARCHAR) AS NAME,
            TRY_CAST(PRICE AS FLOAT) AS PRICE,
            TRY_CAST(PRODUCT_ID AS INTEGER) AS PRODUCT_ID,
            TRY_CAST(SKU AS VARCHAR) AS SKU,
            TRY_CAST(TAGS AS VARCHAR) AS TAGS,
            TRY_CAST(TOTAL_ALLOCATED AS INTEGER) AS TOTAL_ALLOCATED,
            TRY_CAST(TOTAL_AVAILABLE AS INTEGER) AS TOTAL_AVAILABLE,
            TRY_CAST(TOTAL_COMMITTED AS INTEGER) AS TOTAL_COMMITTED,
            TRY_CAST(TOTAL_MFG_ORDERED AS INTEGER) AS TOTAL_MFG_ORDERED,
            TRY_CAST(TOTAL_ON_HAND AS INTEGER) AS TOTAL_ON_HAND,
            TRY_CAST(TOTAL_UNALLOCATED AS INTEGER) AS TOTAL_UNALLOCATED,
            TRY_CAST(TO_BE_SHIPPED AS INTEGER) AS TO_BE_SHIPPED,
            TRY_CAST(UPC AS VARCHAR) AS UPC,
            TRY_CAST(UPDATED AS VARCHAR) AS UPDATED,
            TRY_CAST(WEIGHT AS FLOAT) AS WEIGHT,
            TRY_CAST(WIDTH AS FLOAT) AS WIDTH
        FROM SKU_PROFIT_PROJECT.ERD.INVENTORY_PRODUCT_LEVEL_SNAP
    """,

    # --- 关键修复 ---
    # 同样对仓库级别的库存数据应用 TRY_CAST，确保数据加载的完整性和稳定性。
    "inventory_warehouse_level_snap": """
        SELECT
            TRY_CAST(ALLOCATED AS INTEGER) AS ALLOCATED,
            TRY_CAST(AOH AS INTEGER) AS AOH,
            TRY_CAST(ASIN AS VARCHAR) AS ASIN,
            TRY_CAST(CARTS AS VARCHAR) AS CARTS,
            TRY_CAST(CMT AS INTEGER) AS CMT,
            TRY_CAST(COST AS FLOAT) AS COST,
            TRY_CAST(COUNTRY AS VARCHAR) AS COUNTRY,
            TRY_CAST(HEIGHT AS FLOAT) AS HEIGHT,
            TRY_CAST(LOCATION AS VARCHAR) AS LOCATION,
            TRY_CAST(LOW_STOCK_THTD AS INTEGER) AS LOW_STOCK_THTD,
            TRY_CAST(NAME AS VARCHAR) AS NAME,
            TRY_CAST(OMO AS INTEGER) AS OMO,
            TRY_CAST(ON_HAND AS INTEGER) AS ON_HAND,
            TRY_CAST(OOS_THTD AS INTEGER) AS OOS_THTD,
            TRY_CAST(OPO AS INTEGER) AS OPO,
            TRY_CAST(POH AS INTEGER) AS POH,
            TRY_CAST(PRICE AS FLOAT) AS PRICE,
            TRY_CAST(PRODUCT_ID AS INTEGER) AS PRODUCT_ID,
            TRY_CAST(SKU AS VARCHAR) AS SKU,
            TRY_CAST(TAGS AS VARCHAR) AS TAGS,
            TRY_CAST(TOTAL_ALLOCATED AS INTEGER) AS TOTAL_ALLOCATED,
            TRY_CAST(TOTAL_AVAILABLE AS INTEGER) AS TOTAL_AVAILABLE,
            TRY_CAST(TOTAL_COMMITTED AS INTEGER) AS TOTAL_COMMITTED,
            TRY_CAST(TOTAL_MFG_ORDERED AS INTEGER) AS TOTAL_MFG_ORDERED,
            TRY_CAST(TOTAL_ON_HAND AS INTEGER) AS TOTAL_ON_HAND,
            TRY_CAST(TOTAL_UNALLOCATED AS INTEGER) AS TOTAL_UNALLOCATED,
            TRY_CAST(TO_BE_SHIPPED AS INTEGER) AS TO_BE_SHIPPED,
            TRY_CAST(UNALLOCATED AS INTEGER) AS UNALLOCATED,
            TRY_CAST(UPC AS VARCHAR) AS UPC,
            TRY_CAST(UPDATED AS VARCHAR) AS UPDATED,
            TRY_CAST(WEIGHT AS FLOAT) AS WEIGHT,
            TRY_CAST(WH_CREATED AS VARCHAR) AS WH_CREATED,
            TRY_CAST(WH_ID AS INTEGER) AS WH_ID,
            TRY_CAST(WH_IS_DEFAULT AS BOOLEAN) AS WH_IS_DEFAULT,
            TRY_CAST(WH_LAST_CHANGE AS VARCHAR) AS WH_LAST_CHANGE,
            TRY_CAST(WH_NAME AS VARCHAR) AS WH_NAME,
            TRY_CAST(WH_SHIP_CFG AS BOOLEAN) AS WH_SHIP_CFG,
            TRY_CAST(WH_UPDATED AS VARCHAR) AS WH_UPDATED,
            TRY_CAST(WIDTH AS FLOAT) AS WIDTH
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
