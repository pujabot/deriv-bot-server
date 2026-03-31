import sys
import json
import asyncio
import websockets
import requests
import traceback
from datetime import datetime

async def log_trade(server_url, user_id, session_id, symbol, stake, profit, result_str):
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
    try:
        config_str = sys.stdin.read()
        if not config_str:
            print("No config received", file=sys.stderr)
            return
        config = json.loads(config_str)

        TOKEN = config["token"]
        USER_ID = config["userId"]
        SESSION_ID = config["sessionId"]
        BASE_STAKE = float(config["baseStake"])
        MARTINGALE_MULT = float(config["martingaleMult"])
        TAKE_PROFIT = float(config["takeProfit"])
        STOP_LOSS = float(config["stopLoss"])
        SERVER_URL = config["serverUrl"]

        APP_ID = 1089
        WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
        CURRENCY = "USD"
        DURATION = 1
        DURATION_UNIT = "t"
        SYMBOL = "1HZ100V"

        async def send(ws, data):
            await ws.send(json.dumps(data))

        async def recv(ws):
            return json.loads(await ws.recv())

        async def wait_msg(ws, msg_type):
            while True:
                msg = await recv(ws)
                if "error" in msg:
                    print(f"API ERROR: {msg}", file=sys.stderr)
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

        async def get_result(ws, cid):
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
                print(f"Authorization failed: {auth_msg}", file=sys.stderr)
                return
            print("Authorized successfully")

            stake = BASE_STAKE
            session_profit = 0.0
            trade_count = 0
            loss_streak = 0
            mode = "OVER2"

            while True:
                if session_profit >= TAKE_PROFIT:
                    print(f"Take profit reached: ${session_profit:.2f}")
                    break
                if session_profit <= STOP_LOSS:
                    print(f"Stop loss hit: ${session_profit:.2f}")
                    break
                if loss_streak >= 6:
                    print("Too many consecutive losses, stopping.")
                    break

                if mode == "OVER2":
                    cid = await buy(ws, stake, "DIGITOVER", "2")
                    profit = await get_result(ws, cid)
                    session_profit += profit
                    trade_count += 1
                    result_str = "WIN" if profit > 0 else "LOSS"
                    await log_trade(SERVER_URL, USER_ID, SESSION_ID, SYMBOL, stake, profit, result_str)
                    print(f"Trade {trade_count} [{result_str}]: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake:.2f}")

                    if profit > 0:
                        stake = BASE_STAKE
                        loss_streak = 0
                    else:
                        stake *= MARTINGALE_MULT
                        loss_streak += 1
                        mode = "UNDER6"

                elif mode == "UNDER6":
                    for _ in range(3):
                        cid = await buy(ws, stake, "DIGITUNDER", "6")
                        profit = await get_result(ws, cid)
                        session_profit += profit
                        trade_count += 1
                        result_str = "WIN" if profit > 0 else "LOSS"
                        await log_trade(SERVER_URL, USER_ID, SESSION_ID, SYMBOL, stake, profit, result_str)
                        print(f"Recovery [{result_str}]: profit ${profit:.2f} | session ${session_profit:.2f}")

                        if profit > 0:
                            stake = BASE_STAKE
                            loss_streak = 0
                            break
                        else:
                            stake *= MARTINGALE_MULT
                            loss_streak += 1
                            if loss_streak >= 6:
                                break

                    mode = "OVER2"

            print(f"Bot finished. Final session profit: ${session_profit:.2f}")

    except Exception as e:
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

if __name__ == "__main__":
    asyncio.run(main())
