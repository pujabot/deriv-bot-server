import sys
import json
import asyncio
import websockets
import time
import requests
from datetime import datetime

async def main():
    # Read configuration from stdin (sent by server.py)
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
    STUDY_TICKS = 40
    THRESHOLD = 0.80
    LOSS_SWITCH_THRESHOLD = 6
    SYMBOLS = {
        "V10": "1HZ10V",
        "V25": "1HZ25V",
        "V50": "1HZ50V",
        "V75": "1HZ75V",
        "V100": "1HZ100V"
    }

    overall_profit = 0

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

    async def heartbeat(ws):
        while True:
            await send(ws, {"ping": 1})
            await asyncio.sleep(30)

    async def study_symbol(ws, symbol):
        await send(ws, {"ticks": symbol, "subscribe": 1})
        over2 = 0
        for _ in range(STUDY_TICKS):
            tick = await wait_msg(ws, "tick")
            d = last_digit(tick["tick"]["quote"])
            if d > 2:
                over2 += 1
        await send(ws, {"forget_all": "ticks"})
        return over2 / STUDY_TICKS

    async def buy(ws, symbol, amount, ctype, barrier):
        await send(ws, {
            "proposal": 1,
            "amount": amount,
            "basis": "stake",
            "contract_type": ctype,
            "currency": CURRENCY,
            "duration": DURATION,
            "duration_unit": DURATION_UNIT,
            "symbol": symbol,
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

    async def log_trade(symbol, stake, profit, result_str):
        """Send trade info back to Flask server"""
        try:
            requests.post(f"{SERVER_URL}/log_trade", json={
                "userId": USER_ID,
                "sessionId": SESSION_ID,
                "symbol": symbol,
                "stake": stake,
                "profit": profit,
                "result": result_str,
                "timestamp": datetime.utcnow().isoformat()
            }, timeout=5)
        except Exception as e:
            print(f"Failed to log trade: {e}", file=sys.stderr)

    # ---------- Main bot logic ----------
    async with websockets.connect(WS_URL) as ws:
        await send(ws, {"authorize": TOKEN})
        auth_msg = await wait_msg(ws, "authorize")
        if "error" in auth_msg:
            print("Authorization failed", file=sys.stderr)
            return
        print("✅ Authorized")
        asyncio.create_task(heartbeat(ws))

        # Scan best volatility
        best_name, best_symbol, best_percent = None, None, 0
        for name, sym in SYMBOLS.items():
            pct = await study_symbol(ws, sym)
            print(f"{name} OVER2% = {pct*100:.2f}%")
            if pct > best_percent:
                best_percent = pct
                best_name, best_symbol = name, sym
        if best_percent < THRESHOLD:
            print("No good volatility")
            return

        print(f"Trading on {best_name}")
        stake = BASE_STAKE
        mode = "OVER2"
        trade_count = 0
        loss_streak = 0
        session_profit = 0.0
        trade_limit_reached = False
        MAX_TRADES = 10  # optional limit

        while True:
            if session_profit >= TAKE_PROFIT:
                print(f"Take profit reached: ${session_profit:.2f}")
                break
            if session_profit <= STOP_LOSS:
                print(f"Stop loss hit: ${session_profit:.2f}")
                break

            if mode == "OVER2":
                cid = await buy(ws, best_symbol, stake, "DIGITOVER", "2")
                profit = await result(ws, cid)
                session_profit += profit
                overall_profit += profit
                trade_count += 1
                await log_trade(best_symbol, stake, profit, "WIN" if profit > 0 else "LOSS")
                print(f"Trade profit: {profit:.2f} | Session: {session_profit:.2f}")
                if profit > 0:
                    loss_streak = 0
                    stake *= MARTINGALE_MULT
                else:
                    loss_streak += 1
                    mode = "UNDER6"
            elif mode == "UNDER6":
                for _ in range(3):
                    cid = await buy(ws, best_symbol, stake, "DIGITUNDER", "6")
                    profit = await result(ws, cid)
                    session_profit += profit
                    overall_profit += profit
                    trade_count += 1
                    await log_trade(best_symbol, stake, profit, "WIN" if profit > 0 else "LOSS")
                    print(f"Recovery trade profit: {profit:.2f}")
                    if profit > 0:
                        loss_streak = 0
                        break
                    else:
                        loss_streak += 1
                stake = BASE_STAKE
                mode = "OVER2"

            if loss_streak >= LOSS_SWITCH_THRESHOLD:
                print("Losing streak, rescanning...")
                # Rescan volatility (simplified)
                break  # exit and let main loop restart

            if trade_count >= MAX_TRADES and not trade_limit_reached:
                trade_limit_reached = True
                if session_profit > 0:
                    break

if __name__ == "__main__":
    asyncio.run(main())
