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

    data = request.get_json(silent=True)

    if not data:
        return jsonify({"status": "No JSON received"}), 400

    token = data.get("token")

    if not token:
        return jsonify({"status": "Token missing"}), 400

    if bot_process is None:
        bot_process = subprocess.Popen(["python", "bot.py", token])
        return jsonify({"status": "Bot started"})
    else:
        return jsonify({"status": "Bot already running"})


@app.route("/stop", methods=["POST"])
def stop_bot():
    global bot_process

    if bot_process:
        bot_process.terminate()
        bot_process = None
        return jsonify({"status": "Bot stopped"})
    else:
        return jsonify({"status": "Bot not running"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
