import os
import json
from base64 import b64decode
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from cryptography.hazmat.primitives import serialization

app = Flask(__name__)
CORS(app) # 允许来自任何源的跨域请求

def get_snowflake_connection():
    """
    从环境变量安全地获取凭据并连接到 Snowflake。
    """
    try:
        # 从环境变量加载私钥内容
        private_key_str = os.environ.get('PRIVATE_KEY_STR')
        if not private_key_str:
            raise ValueError("PRIVATE_KEY_STR 环境变量未设置。")

        # 从环境变量加载私钥密码
        private_key_passphrase_str = os.environ.get('PRIVATE_KEY_PASSPHRASE')
        if not private_key_passphrase_str:
            raise ValueError("PRIVATE_KEY_PASSPHRASE 环境变量未设置。")
        
        private_key_passphrase = private_key_passphrase_str.encode()

        p_key = serialization.load_pem_private_key(
            private_key_str.encode(),
            password=private_key_passphrase,
        )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        conn = snowflake.connector.connect(
            user=os.environ.get('SNOWFLAKE_USER'),
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            private_key=pkb,
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            schema=os.environ.get('SNOWFLAKE_SCHEMA'),
        )
        return conn
    except Exception as e:
        print(f"连接 Snowflake 时出错: {e}")
        # 在实际生产中，您可能希望有更复杂的日志记录
        raise

@app.route('/api/query', methods=['POST'])
def query_snowflake():
    """
    接收 SQL 查询，在 Snowflake 上执行，并以 JSON 格式返回结果。
    """
    data = request.get_json()
    sql_query = data.get('sql')

    if not sql_query:
        return jsonify({"message": "请求中缺少 'sql' 参数。"}), 400

    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor(snowflake.connector.cursor.DictCursor)
        cur.execute(sql_query)
        result = cur.fetchall()
        
        # 将结果转换为 JSON (DictCursor 已经完成了大部分工作)
        return jsonify(result)

    except Exception as e:
        return jsonify({"message": f"执行查询时出错: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # 这部分仅用于本地测试，在 Render 上会使用 gunicorn 启动
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5001)))
