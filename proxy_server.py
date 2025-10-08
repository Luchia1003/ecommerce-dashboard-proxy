import os
import base64
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
CORS(app)

def get_snowflake_connection():
    try:
        # 1. Get the Base64 encoded private key from environment variables
        private_key_str = os.environ.get('PRIVATE_KEY_STR')
        if not private_key_str:
            raise ValueError("环境变量 PRIVATE_KEY_STR 未设置。")
        
        # 2. Decode the Base64 string to bytes
        private_key_bytes = base64.b64decode(private_key_str)

        # 3. CORRECTED LOGIC: Handle both encrypted and unencrypted keys
        # Get passphrase if it exists. If not, passphrase remains None.
        passphrase = os.environ.get('PRIVATE_KEY_PASSPHRASE')
        
        # The `password` argument can be None for unencrypted keys.
        # Encode the passphrase to bytes only if it's not None.
        password_bytes = passphrase.encode() if passphrase else None
        
        # 4. Load the private key
        p_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=password_bytes, # Pass None if key is not encrypted
            backend=default_backend()
        )

        # 5. Get the private key in DER format for the Snowflake connector
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # 6. Connect to Snowflake
        conn = snowflake.connector.connect(
            user=os.environ.get('SNOWFLAKE_USER'),
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            private_key=pkb,
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            schema=os.environ.get('SNOWFLAKE_SCHEMA')
        )
        return conn
        
    except Exception as e:
        # Catch all errors during key deserialization or connection
        # and wrap them in an informative message for easier debugging.
        logging.error(f"Failed to connect to Snowflake: {e}")
        # Re-raise the exception to be caught by the route handler
        raise Exception(f"连接 Snowflake 时出错: {e}")


@app.route('/api/query', methods=['POST'])
def query_snowflake():
    try:
        data = request.get_json()
        sql = data.get('sql')

        if not sql:
            return jsonify({'error': 'SQL query is missing.'}), 400

        conn = get_snowflake_connection()
        cursor = conn.cursor(snowflake.connector.DictCursor)
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify(results)

    except Exception as e:
        # This catches connection errors etc.
        logging.error(f"Error during query execution: {e}") # Log to Render
        # Return the specific error message from get_snowflake_connection or query execution
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
