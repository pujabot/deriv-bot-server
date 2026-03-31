import os
import json
import subprocess
import uuid
import tempfile
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore

app = Flask(__name__)
CORS(app)

# ---------- Firebase Initialization ----------
firebase_cred_json = os.environ.get("FIREBASE_CREDENTIALS_JSON")
if firebase_cred_json:
    # Write JSON to a temp file and load it
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(firebase_cred_json)
        cred_path = f.name
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    os.unlink(cred_path)  # delete temp file after loading
else:
    # For local testing with GOOGLE_APPLICATION_CREDENTIALS
    try:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
    except Exception as e:
        print("ERROR: No Firebase credentials. Set FIREBASE_CREDENTIALS_JSON env var.")
        raise

db = firestore.client()

# Store running bot processes (key = user_id)
running_bots = {}

@app.route("/")
def home():
    return "Deriv Bot Server Running"

@app.route("/balance", methods=["POST"])
def get_balance():
    data = request.get_json()
    user_id = data.get("userId")
    token = data.get("token")
    if not user_id or not token:
        return jsonify({"error": "Missing userId or token"}), 400
    try:
        proc = subprocess.run(
            ["python", "balance_check.py", token],
            capture_output=True, text=True, timeout=15
        )
        if proc.returncode == 0 and proc.stdout.strip():
            balance = float(proc.stdout.strip())
            return jsonify({"balance": balance})
        else:
            return jsonify({"error": proc.stderr or "Invalid token"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/start", methods=["POST"])
def start_bot():
    data = request.get_json()
    if not data:
        return jsonify({"status": "Invalid request"}), 400

    user_id = data.get("userId")
    token = data.get("token")
    settings = data.get("settings", {})

    if not user_id or not token:
        return jsonify({"status": "Missing userId or token"}), 400

    # Check trial / subscription
    user_ref = db.collection("users").document(user_id)
    user_doc = user_ref.get()
    if not user_doc.exists:
        return jsonify({"status": "User not found"}), 404

    user_data = user_doc.to_dict()
    trial_expiry_str = user_data.get("trialExpiry")
    subscription_active = user_data.get("subscriptionActive", False)

    now = datetime.utcnow()
    trial_valid = False
    if trial_expiry_str:
        trial_expiry = datetime.fromisoformat(trial_expiry_str)
        if now < trial_expiry:
            trial_valid = True

    if not (trial_valid or subscription_active):
        return jsonify({"status": "Trial expired or no active subscription"}), 403

    # Check if bot already running
    if user_id in running_bots and running_bots[user_id].poll() is None:
        return jsonify({"status": "Bot already running for this user"})

    # Prepare settings
    base_stake = settings.get("baseStake", 0.35)
    martingale_mult = settings.get("martingaleMult", 4.0)
    take_profit = settings.get("takeProfit", 10.0)
    stop_loss = settings.get("stopLoss", -5.0)
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
        ["python", "bot.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    proc.stdin.write(json.dumps(bot_input))
    proc.stdin.close()
    running_bots[user_id] = proc

    return jsonify({"status": f"Bot started (session {session_id})"})

@app.route("/stop", methods=["POST"])
def stop_bot():
    data = request.get_json()
    user_id = data.get("userId") if data else None
    if not user_id:
        return jsonify({"status": "Missing userId"}), 400

    proc = running_bots.get(user_id)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        del running_bots[user_id]
        return jsonify({"status": "Bot stopped"})
    else:
        return jsonify({"status": "No bot running for this user"})

@app.route("/log_trade", methods=["POST"])
def log_trade():
    data = request.get_json()
    required = ["userId", "sessionId", "symbol", "stake", "profit", "result"]
    if not all(k in data for k in required):
        return jsonify({"status": "Missing fields"}), 400

    trade_ref = db.collection("trades").document()
    trade_ref.set({
        "userId": data["userId"],
        "sessionId": data["sessionId"],
        "symbol": data["symbol"],
        "stake": data["stake"],
        "profit": data["profit"],
        "result": data["result"],
        "timestamp": firestore.SERVER_TIMESTAMP,
        "raw_time": datetime.utcnow().isoformat()
    })
    return jsonify({"status": "logged"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
