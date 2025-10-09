import os
import base64
import snowflake.connector
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
from cryptography.hazmat.primitives import serialization

# 初始化 Flask 应用
app = Flask(__name__)
# 允许所有来源的跨域请求，这对于连接前端至关重要
CORS(app)

# 定义一个函数来获取 Snowflake 连接
def get_snowflake_connection():
    # 从环境变量中解码 Base64 编码的私钥
    private_key_b64 = os.getenv('PRIVATE_KEY_STR')
    if not private_key_b64:
        raise ValueError("PRIVATE_key_STR environment variable not set")

    private_key_bytes = base64.b64decode(private_key_b64)
    
    # 使用 cryptography 库加载私钥
    p_key = serialization.load_pem_private_key(
        private_key_bytes,
        password=None,  # 如果你的私钥有密码，请在这里设置
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # 创建 Snowflake 连接
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USERNAME'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        private_key=pkb,
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    return conn

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
