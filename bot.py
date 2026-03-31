import sys
import json
import asyncio
import websockets
import requests
from datetime import datetime

async def log_trade(server_url, user_id, session_id, symbol, stake, profit, result_str):
    """Send trade info to Flask server asynchronously"""
    try:
        await asyncio.to_thread(
            requests.post,
            f"{server_url}/log_trade",
            json={
                "userId": user_id,
                "sessionId": session_id,
                "symbol": symbol,
                "stake": stake,
                "profit": profit,
                "result": result_str,
                "raw_time": datetime.utcnow().isoformat()
            },
            timeout=5
        )
    except Exception as e:
        print(f"Logging failed: {e}", file=sys.stderr)

async def main():
    # Read config from stdin
    config_str = sys.stdin.read()
    if not config_str:
        print("No config received", file=sys.stderr)
        return
    config = json.loads(config_str)

    TOKEN = config["token"]
    USER_ID = config["userId"]
    SESSION_ID = config["sessionId"]
    BASE_STAKE = config["baseStake"]
    MARTINGALE_MULT = config["martingaleMult"]
    TAKE_PROFIT = config["takeProfit"]
    STOP_LOSS = config["stopLoss"]
    SERVER_URL = config["serverUrl"]

    APP_ID = 1089
    WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
    CURRENCY = "USD"
    DURATION = 1
    DURATION_UNIT = "t"
    SYMBOL = "1HZ100V"  # Using a single volatile symbol for simplicity

    def last_digit(price):
        return int(str(price).replace(".", "")[-1])

    async def send(ws, data):
        await ws.send(json.dumps(data))

    async def recv(ws):
        msg = await ws.recv()
        return json.loads(msg)

    async def wait_msg(ws, msg_type):
        while True:
            msg = await recv(ws)
            if "error" in msg:
                print("API ERROR:", msg, file=sys.stderr)
            if msg.get("msg_type") == msg_type:
                return msg

    async def buy(ws, amount, ctype, barrier):
        await send(ws, {
            "proposal": 1,
            "amount": amount,
            "basis": "stake",
            "contract_type": ctype,
            "currency": CURRENCY,
            "duration": DURATION,
            "duration_unit": DURATION_UNIT,
            "symbol": SYMBOL,
            "barrier": str(barrier)
        })
        proposal = await wait_msg(ws, "proposal")
        pid = proposal["proposal"]["id"]
        await send(ws, {"buy": pid, "price": amount})
        buy_msg = await wait_msg(ws, "buy")
        return buy_msg["buy"]["contract_id"]

    async def result(ws, cid):
        await send(ws, {
            "proposal_open_contract": 1,
            "contract_id": cid,
            "subscribe": 1
        })
        while True:
            msg = await wait_msg(ws, "proposal_open_contract")
            if msg["proposal_open_contract"]["is_sold"]:
                return float(msg["proposal_open_contract"]["profit"])

    async with websockets.connect(WS_URL) as ws:
        await send(ws, {"authorize": TOKEN})
        auth_msg = await wait_msg(ws, "authorize")
        if "error" in auth_msg:
            print("Authorization failed", file=sys.stderr)
            return
        print("✅ Authorized")

        stake = BASE_STAKE
        session_profit = 0.0
        trade_count = 0
        loss_streak = 0
        mode = "OVER2"

        while True:
            # Check take profit / stop loss
            if session_profit >= TAKE_PROFIT:
                print(f"Take profit reached: ${session_profit:.2f}")
                break
            if session_profit <= STOP_LOSS:
                print(f"Stop loss hit: ${session_profit:.2f}")
                break

            if mode == "OVER2":
                cid = await buy(ws, stake, "DIGITOVER", "2")
                profit = await result(ws, cid)
                session_profit += profit
                trade_count += 1
                await log_trade(SERVER_URL, USER_ID, SESSION_ID, SYMBOL, stake, profit, "WIN" if profit > 0 else "LOSS")
                print(f"Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f}")
                if profit > 0:
                    loss_streak = 0
                    stake *= MARTINGALE_MULT
                else:
                    loss_streak += 1
                    mode = "UNDER6"
            elif mode == "UNDER6":
                # Recovery trades
                for _ in range(3):
                    cid = await buy(ws, stake, "DIGITUNDER", "6")
                    profit = await result(ws, cid)
                    session_profit += profit
                    trade_count += 1
                    await log_trade(SERVER_URL, USER_ID, SESSION_ID, SYMBOL, stake, profit, "WIN" if profit > 0 else "LOSS")
                    print(f"Recovery trade: profit ${profit:.2f} | session ${session_profit:.2f}")
                    if profit > 0:
                        loss_streak = 0
                        break
                    else:
                        loss_streak += 1
                stake = BASE_STAKE
                mode = "OVER2"

            if loss_streak >= 6:
                print("Too many losses, stopping")
                break

        print(f"Bot finished. Final profit: ${session_profit:.2f}")

if __name__ == "__main__":
    asyncio.run(main())
