import os
import sys
import json
import subprocess
import uuid
import tempfile
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore

app = Flask(__name__)

CORS(app, resources={r"/*": {
    "origins": "*",
    "methods": ["GET", "POST", "OPTIONS"],
    "allow_headers": ["Content-Type", "Authorization"]
}})

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        res = app.make_default_options_response()
        res.headers["Access-Control-Allow-Origin"] = "*"
        res.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        res.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        return res

def cors_response(data, status=200):
    res = jsonify(data)
    res.status_code = status
    res.headers["Access-Control-Allow-Origin"] = "*"
    res.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    res.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return res

# ---------- Firebase ----------
firebase_cred_json = os.environ.get("FIREBASE_CREDENTIALS_JSON")
if firebase_cred_json:
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(firebase_cred_json)
        cred_path = f.name
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    os.unlink(cred_path)
else:
    try:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
    except Exception as e:
        print("ERROR: No Firebase credentials. Set FIREBASE_CREDENTIALS_JSON env var.")
        raise

db = firestore.client()
running_bots = {}

@app.route("/")
def home():
    return "Deriv Bot Server Running"

@app.route("/balance", methods=["POST", "OPTIONS"])
def get_balance():
    if request.method == "OPTIONS":
        return cors_response({})
    data = request.get_json()
    if not data:
        return cors_response({"error": "No data received"}, 400)
    user_id = data.get("userId")
    token = data.get("token")
    if not user_id or not token:
        return cors_response({"error": "Missing userId or token"}, 400)
    try:
        proc = subprocess.run(
            [sys.executable, "balance_check.py", token],
            capture_output=True, text=True, timeout=20
        )
        output = proc.stdout.strip()
        if proc.returncode == 0 and output:
            try:
                balance = float(output)
                if balance < 0:
                    return cors_response({"error": "Invalid token or authorization failed"}, 401)
                return cors_response({"balance": balance})
            except ValueError:
                return cors_response({"error": "Unexpected output: " + output}, 500)
        else:
            err = proc.stderr.strip() or "balance_check.py returned no output"
            return cors_response({"error": err}, 500)
    except subprocess.TimeoutExpired:
        return cors_response({"error": "Balance check timed out"}, 504)
    except Exception as e:
        return cors_response({"error": str(e)}, 500)

@app.route("/start", methods=["POST", "OPTIONS"])
def start_bot():
    if request.method == "OPTIONS":
        return cors_response({})
    data = request.get_json()
    if not data:
        return cors_response({"status": "Invalid request"}, 400)

    user_id = data.get("userId")
    token = data.get("token")
    settings = data.get("settings", {})

    if not user_id or not token:
        return cors_response({"status": "Missing userId or token"}, 400)

    user_ref = db.collection("users").document(user_id)
    user_doc = user_ref.get()
    if not user_doc.exists:
        return cors_response({"status": "User not found"}, 404)

    user_data = user_doc.to_dict()
    trial_expiry_str = user_data.get("trialExpiry")
    subscription_active = user_data.get("subscriptionActive", False)

    now = datetime.now(timezone.utc)
    trial_valid = False
    if trial_expiry_str:
        try:
            trial_expiry = datetime.fromisoformat(trial_expiry_str)
            if trial_expiry.tzinfo is None:
                trial_expiry = trial_expiry.replace(tzinfo=timezone.utc)
            if now < trial_expiry:
                trial_valid = True
        except Exception:
            trial_valid = False

    if not (trial_valid or subscription_active):
        return cors_response({"status": "Trial expired or no active subscription"}, 403)

    if user_id in running_bots and running_bots[user_id].poll() is None:
        return cors_response({"status": "Bot already running for this user"})

    base_stake = float(settings.get("baseStake", 0.35))
    martingale_mult = float(settings.get("martingaleMult", 4.0))
    take_profit = float(settings.get("takeProfit", 10.0))
    stop_loss = float(settings.get("stopLoss", -5.0))
    session_id = str(uuid.uuid4())

    bot_input = {
        "token": token,
        "userId": user_id,
        "sessionId": session_id,
        "baseStake": base_stake,
        "martingaleMult": martingale_mult,
        "takeProfit": take_profit,
        "stopLoss": stop_loss,
        "serverUrl": request.host_url.rstrip('/')
    }

    proc = subprocess.Popen(
        [sys.executable, "bot.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    proc.stdin.write(json.dumps(bot_input))
    proc.stdin.close()

    # Start a thread to capture stderr and print it (so Render logs show errors)
    def log_stderr():
        for line in iter(proc.stderr.readline, ''):
            print(f"[BOT ERR] {line.strip()}", flush=True)
        proc.stderr.close()
    import threading
    t = threading.Thread(target=log_stderr)
    t.daemon = True
    t.start()

    running_bots[user_id] = proc
    return cors_response({"status": f"Bot started (session {session_id})"})

@app.route("/stop", methods=["POST", "OPTIONS"])
def stop_bot():
    if request.method == "OPTIONS":
        return cors_response({})
    data = request.get_json()
    user_id = data.get("userId") if data else None
    if not user_id:
        return cors_response({"status": "Missing userId"}, 400)

    proc = running_bots.get(user_id)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        del running_bots[user_id]
        return cors_response({"status": "Bot stopped"})
    else:
        return cors_response({"status": "No bot running for this user"})

@app.route("/log_trade", methods=["POST", "OPTIONS"])
def log_trade():
    if request.method == "OPTIONS":
        return cors_response({})
    data = request.get_json()
    required = ["userId", "sessionId", "symbol", "stake", "profit", "result"]
    if not data or not all(k in data for k in required):
        return cors_response({"status": "Missing fields"}, 400)

    trade_ref = db.collection("trades").document()
    trade_ref.set({
        "userId": data["userId"],
        "sessionId": data["sessionId"],
        "symbol": data["symbol"],
        "stake": data["stake"],
        "profit": data["profit"],
        "result": data["result"],
        "timestamp": firestore.SERVER_TIMESTAMP,
        "raw_time": datetime.now(timezone.utc).isoformat()
    })
    return cors_response({"status": "logged"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
