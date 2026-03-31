import os
import json
import subprocess
import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore

app = Flask(__name__)
CORS(app)

# Initialize Firebase Admin SDK
# Make sure to set environment variable GOOGLE_APPLICATION_CREDENTIALS
cred = credentials.ApplicationDefault()
if not cred:
    # Fallback: try to load from file (for local testing)
    try:
        cred = credentials.Certificate("firebase-adminsdk.json")
    except:
        print("ERROR: No Firebase credentials found. Set GOOGLE_APPLICATION_CREDENTIALS.")
firebase_admin.initialize_app(cred)
db = firestore.client()

# Store running bot processes (in production use a proper task queue)
# key = userId, value = subprocess.Popen
running_bots = {}

@app.route("/")
def home():
    return "Deriv Bot Server Running"

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
        return jsonify({"status": "Trial expired or no active subscription. Please subscribe."}), 403

    # Check if bot already running for this user
    if user_id in running_bots and running_bots[user_id].poll() is None:
        return jsonify({"status": "Bot already running for this user"})

    # Prepare settings with defaults
    base_stake = settings.get("baseStake", 0.35)
    martingale_mult = settings.get("martingaleMult", 4.0)
    take_profit = settings.get("takeProfit", 10.0)
    stop_loss = settings.get("stopLoss", -5.0)

    # Create a unique session ID
    session_id = str(uuid.uuid4())

    # Launch bot.py as subprocess, pass all data as JSON
    bot_input = {
        "token": token,
        "userId": user_id,
        "sessionId": session_id,
        "baseStake": base_stake,
        "martingaleMult": martingale_mult,
        "takeProfit": take_profit,
        "stopLoss": stop_loss,
        "serverUrl": request.host_url.rstrip('/')  # so bot can call back
    }
    # Use stdin to pass config to avoid command line length limits
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

    return jsonify({"status": f"Bot started for session {session_id}"})

@app.route("/stop", methods=["POST"])
def stop_bot():
    data = request.get_json()
    user_id = data.get("userId") if data else None
    if not user_id:
        return jsonify({"status": "Missing userId"}), 400
    
    proc = running_bots.get(user_id)
    if proc and proc.poll() is None:
        proc.terminate()
        # Wait a bit then force kill if needed
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
    """Endpoint for bot.py to report trade results"""
    data = request.get_json()
    required = ["userId", "sessionId", "symbol", "stake", "profit", "result", "timestamp"]
    if not all(k in data for k in required):
        return jsonify({"status": "Missing fields"}), 400
    
    # Store trade in Firestore
    trade_ref = db.collection("trades").document()
    trade_ref.set({
        "userId": data["userId"],
        "sessionId": data["sessionId"],
        "symbol": data["symbol"],
        "stake": data["stake"],
        "profit": data["profit"],
        "result": data["result"],
        "timestamp": firestore.SERVER_TIMESTAMP,
        "raw_time": data["timestamp"]
    })
    return jsonify({"status": "logged"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
