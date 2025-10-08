import os
import base64
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("proxy")

app = Flask(__name__)

# ===== CORS（可选：限定来源域名）=====
allowed_origin = os.getenv("ALLOWED_ORIGIN")  # e.g. https://your-dashboard.example
if allowed_origin:
    CORS(app, resources={r"/api/*": {"origins": [allowed_origin]}}, supports_credentials=False)
else:
    CORS(app)  # 宽松（开发期）

# ---------- helpers ----------
def _load_private_key_from_env() -> bytes:
    """从 PRIVATE_KEY_STR 读私钥（支持 base64 或原始 PEM），返回 DER(PKCS8)"""
    key_str = os.environ.get("PRIVATE_KEY_STR")
    if not key_str:
        raise ValueError("ENV PRIVATE_KEY_STR is missing")

    raw: bytes
    # 先尝试 base64
    try:
        raw = base64.b64decode(key_str)
        if b"-----BEGIN" in raw:
            # 传的是 base64 后仍带 PEM 头，等同普通 PEM
            pass
    except Exception:
        # 不是 base64，就当原始 PEM
        raw = key_str.encode("utf-8")

    passphrase = os.environ.get("PRIVATE_KEY_PASSPHRASE")
    password_bytes = passphrase.encode() if passphrase else None

    p_key = serialization.load_pem_private_key(
        raw, password=password_bytes, backend=default_backend()
    )
    der = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return der

def get_snowflake_connection():
    # 关键参数
    account  = os.getenv("SNOWFLAKE_ACCOUNT")     # e.g. RRCWSFA-BSB89302
    host     = os.getenv("SNOWFLAKE_HOST")        # e.g. RRCWSFA-BSB89302.snowflakecomputing.com (可选)
    user     = os.getenv("SNOWFLAKE_USER")
    role     = os.getenv("SNOWFLAKE_ROLE")        # e.g. DASHBOARD_READONLY
    warehouse= os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema   = os.getenv("SNOWFLAKE_SCHEMA")

    if not all([account, user, warehouse, database, schema]):
        raise ValueError("Missing one of required envs: SNOWFLAKE_ACCOUNT/USER/WAREHOUSE/DATABASE/SCHEMA")

    pkb = _load_private_key_from_env()

    try:
        log.info(
            "Connecting to Snowflake | account=%s host=%s user=%s role=%s wh=%s db=%s schema=%s",
            account, host or "(default)", user, role or "(default)", warehouse, database, schema
        )
        conn = snowflake.connector.connect(
            account=account,
            host=host,  # 可为 None
            user=user,
            private_key=pkb,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            login_timeout=20,
            network_timeout=20,
            client_session_keep_alive=True,
            application="RenderProxy/1.0",
        )
        # 快速验证一下连通性
        with conn.cursor() as cur:
            cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_ROLE()")
            cur.fetchone()
        return conn
    except snowflake.connector.errors.Error as e:
        log.error("Snowflake error: number=%s code=%s state=%s msg=%s",
                  getattr(e, 'errno', None), getattr(e, 'sqlstate', None), getattr(e, 'sfqid', None), str(e))
        raise
    except Exception as e:
        log.error("Failed to connect to Snowflake: %s", e)
        raise

# ---------- routes ----------
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True})

@app.route("/api/query", methods=["POST"])
def api_query():
    try:
        data = request.get_json(silent=True) or {}
        sql = (data.get("sql") or "").strip()

        if not sql:
            return jsonify({"error": "SQL is missing"}), 400

        # 简单保护：只允许 SELECT（你也可以移除）
        if not sql[:6].upper() == "SELECT":
            return jsonify({"error": "Only SELECT statements are allowed"}), 400

        conn = get_snowflake_connection()
        try:
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            return jsonify(rows)
        finally:
            conn.close()
    except snowflake.connector.errors.Error as e:
        log.error("Query failed: %s", e)
        return jsonify({"error": f"Snowflake error: {e}"}), 500
    except Exception as e:
        log.error("Error during query execution: %s", e)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
