import os
import base64
import logging  # 新增: 导入日志模块，解决 NameError
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
from snowflake.connector import connect, DatabaseError # 新增: 导入 connect 和 DatabaseError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend # 新增: 导入 default_backend

# 初始化 Flask 应用
app = Flask(__name__)
# 允许所有来源的跨域请求
CORS(app)

# 将日志配置移到所有函数定义之前，确保它在应用启动时就设置好了
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_snowflake_connection():
    try:
        logging.info("Attempting to decode private key from environment variable...")
        
        # 1. 解码 Base64 密钥
        # 这一步是新的、关键的错误捕捉点
        private_key_b64 = os.environ.get('PRIVATE_KEY_STR')
        if not private_key_b64:
            raise ValueError("Environment variable PRIVATE_KEY_STR is not set.")
            
        private_key_bytes = base64.b64decode(private_key_b64)
        logging.info("Private key successfully decoded from Base64.")

        # 2. 从字节中加载私钥
        # 这一步验证密钥格式是否正确
        p_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=None, # 如果你的密钥有密码，在这里填写
            backend=default_backend()
        )
        logging.info("Private key object successfully loaded.")

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        logging.info("Private key converted to DER format for connection.")

        logging.info("Connecting to Snowflake...")
        conn = connect(
            user=os.environ.get('SNOWFLAKE_USERNAME'),
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            private_key=pkb,
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            schema=os.environ.get('SNOWFLAKE_SCHEMA'),
            role=os.environ.get('SNOWFLAKE_ROLE'),
        )
        logging.info("Successfully connected to Snowflake.")
        return conn

    except (ValueError, TypeError) as b64_error:
        # 如果 Base64 解码失败，我们会在这里看到清晰的日志
        logging.critical(f"CRITICAL ERROR: Failed to decode or parse the private key. This is the most likely cause of the problem. Error: {b64_error}")
        logging.critical("Please re-generate the Base64 string for PRIVATE_KEY_STR and ensure it is copied correctly.")
        raise
    except DatabaseError as db_err:
        # 捕捉 Snowflake 特定的连接错误
        logging.critical(f"Snowflake DatabaseError occurred: {db_err}")
        raise
    except Exception as e:
        # 捕捉所有其他意外错误
        logging.critical(f"An unexpected error occurred in get_snowflake_connection: {e}")
        raise


# 创建 /api/data 路由
@app.route('/api/data', methods=['GET'])
def get_dashboard_data():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # 定义需要执行的 SQL 查询
        # 注意：请根据你 Snowflake 中真实的表名和列名修改这些查询
        queries = {
            "productCosts": "SELECT sku, description, unit_cost FROM MASTER_COST_FACT",
            "orders": "SELECT order_id, sales_sku, quantity, sales_margin, product_sales, gross_sales, date_time, currency_code, marketplace_name FROM ORDERS_BASE",
            "refunds": "SELECT order_id, sales_sku, quantity, refund_margin, product_refund, gross_refund, date_time, currency_code, marketplace_name FROM REFUNDS_BASE",
            "shipping": "SELECT ship_date, order_id, items, shipping_cost FROM SHIPPING_DATA",
            "inventory": "SELECT seller_sku, item_name, quantity, open_date FROM INVENTORY_DATA"
        }

        # 执行所有查询并将结果存储在字典中
        all_data = {}
        for key, query in queries.items():
            df = pd.read_sql(query, conn)
            # 将 DataFrame 转换为 JSON 记录格式，这正是前端所期望的
            all_data[key] = df.to_dict(orient='records')

        return jsonify(all_data)

    except Exception as e:
        # 如果出错，返回 500 错误和详细信息
        print(f"An error occurred: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        # 确保连接被关闭
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# 添加一个根路由用于健康检查
@app.route('/', methods=['GET'])
def index():
    return "Proxy server is running. Access data at /api/data"

# 当直接运行此脚本时启动服务器
if __name__ == '__main__':
    # Render 会使用 Gunicorn 启动，这个部分主要用于本地测试
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
