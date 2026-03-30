from flask import Flask, request, jsonify

app = Flask(__name__)

running = False

@app.route("/start", methods=["POST"])
def start_bot():

    global running
    running = True

    print("Bot started")

    return jsonify({"status":"bot started"})


@app.route("/stop", methods=["POST"])
def stop_bot():

    global running
    running = False

    print("Bot stopped")

    return jsonify({"status":"bot stopped"})


@app.route("/")
def home():

    return "Bot server running"


app.run(host="0.0.0.0",port=10000)