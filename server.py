from flask import Flask, request, jsonify
from flask_cors import CORS
import subprocess

app = Flask(__name__)
CORS(app)

bot_process = None


@app.route("/")
def home():
    return "Bot server running"


@app.route("/start", methods=["POST"])
def start_bot():
    global bot_process

    data = request.json
    token = data["token"]

    if bot_process is None:
        bot_process = subprocess.Popen(["python", "bot.py", token])
        return jsonify({"status": "bot started"})
    else:
        return jsonify({"status": "bot already running"})


@app.route("/stop", methods=["POST"])
def stop_bot():
    global bot_process

    if bot_process:
        bot_process.terminate()
        bot_process = None
        return jsonify({"status": "bot stopped"})
    else:
        return jsonify({"status": "bot not running"})


app.run(host="0.0.0.0", port=10000)
