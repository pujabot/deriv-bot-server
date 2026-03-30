import os
import json
import asyncio
import websockets
import time
import sys
from collections import deque

token = sys.argv[1]

APP_ID = 1089
WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

CURRENCY = "USD"
BASE_STAKE = 2

MAX_TRADES = 10

DURATION = 1
DURATION_UNIT = "t"

STUDY_TICKS = 25
THRESHOLD = 0.80
EARLY_SCAN = 0.85

LOSS_SWITCH_THRESHOLD = 6

MOMENTUM_TICKS = 5

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
    try:
        msg = await ws.recv()
        return json.loads(msg)
    except websockets.exceptions.ConnectionClosed:
        print("⚠️ WebSocket closed")
        raise
    except asyncio.CancelledError:
        return {}


async def wait_msg(ws, msg_type):

    while True:

        msg = await recv(ws)

        if not msg:
            continue

        if "error" in msg:
            print("API ERROR:", msg)

        if msg.get("msg_type") == msg_type:
            return msg


async def heartbeat(ws):

    while True:
        try:
            await send(ws, {"ping": 1})
            print("❤️ Heartbeat ping")
            await asyncio.sleep(30)
        except:
            break


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


async def momentum_ok(ws, symbol):

    digits = deque(maxlen=MOMENTUM_TICKS)

    await send(ws, {"ticks": symbol, "subscribe": 1})

    while len(digits) < MOMENTUM_TICKS:

        tick = await wait_msg(ws, "tick")
        d = last_digit(tick["tick"]["quote"])
        digits.append(d)

    await send(ws, {"forget_all": "ticks"})

    over2_count = sum(1 for d in digits if d > 2)

    if over2_count >= 4:
        print("🧠 Momentum strong → trade allowed")
        return True

    print("🧠 Momentum weak → skipping trade")
    return False


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

    buy = await wait_msg(ws, "buy")

    return buy["buy"]["contract_id"]


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


async def scan_best_volatility(ws):

    print("\n🔍 Scanning volatilities...")

    results = {}

    for name, symbol in SYMBOLS.items():

        print(f"📊 Studying {name}...")

        percent = await study_symbol(ws, symbol)

        results[name] = (symbol, percent)

        print(f"{name} OVER2 % = {percent*100:.2f}%")

        if percent >= EARLY_SCAN:

            print(f"\n🔥 {name} reached {percent*100:.2f}%")
            return name, symbol, percent

    best = max(results.items(), key=lambda x: x[1][1])

    return best[0], best[1][0], best[1][1]


async def main():

    global overall_profit

    session_profit = 0

    async with websockets.connect(WS_URL) as ws:

        await send(ws, {"authorize": token})
        await wait_msg(ws, "authorize")

        print("✅ Authorized")

        asyncio.create_task(heartbeat(ws))

        best_name, best_symbol, best_percent = await scan_best_volatility(ws)

        if best_percent < THRESHOLD:
            print("❌ No volatility meets threshold")
            return

        print(f"\n🚀 Trading on {best_name}")

        stake = BASE_STAKE
        mode = "OVER2"

        trade_count = 0
        loss_streak = 0
        first_three_wins = 0

        trade_limit_reached = False

        while True:

            if mode == "OVER2":

                if not await momentum_ok(ws, best_symbol):
                    continue

                print(f"\n[{best_name}] 🚀 OVER2 stake={stake}")

                cid = await buy(ws, best_symbol, stake, "DIGITOVER", "2")

                profit = await result(ws, cid)

                session_profit += profit
                overall_profit += profit
                trade_count += 1

                print(f"💰 Trade profit: {profit:.2f}")
                print(f"📊 Session profit: {session_profit:.2f}")
                print(f"🌍 Overall profit: {overall_profit:.2f}")

                if profit > 0:

                    print("✅ WIN")

                    if trade_count <= 3:
                        first_three_wins += 1

                    if first_three_wins == 3:
                        print("\n🔥 First 3 trades all wins")
                        print("🔄 Restarting scan")
                        return

                    loss_streak = 0
                    stake *= 1.5

                else:

                    print("❌ LOSS")

                    loss_streak += 1
                    stake *= 2
                    mode = "UNDER6"

            elif mode == "UNDER6":

                print("\n🔄 UNDER6 recovery")

                for i in range(3):

                    print(f"\n🛠 Recovery trade {i+1}")
                    print(f"💵 Recovery stake = {stake}")

                    cid = await buy(ws, best_symbol, stake, "DIGITUNDER", "6")

                    profit = await result(ws, cid)

                    session_profit += profit
                    overall_profit += profit
                    trade_count += 1

                    print(f"💰 Trade profit: {profit:.2f}")

                    if profit > 0:

                        print("✅ Recovery success")
                        loss_streak = 0
                        break

                    else:

                        print("❌ Recovery loss")
                        stake *= 2
                        loss_streak += 1

                stake = BASE_STAKE
                mode = "OVER2"

            if loss_streak >= LOSS_SWITCH_THRESHOLD:

                print("\n⚠️ Losing streak detected")
                print("🔄 Switching volatility")

                best_name, best_symbol, best_percent = await scan_best_volatility(ws)

                stake = BASE_STAKE
                loss_streak = 0

            if trade_count >= MAX_TRADES and not trade_limit_reached:

                trade_limit_reached = True

                print("\n🔟 10 trades reached")

                if session_profit > 0:
                    print("🎯 Session profit reached")
                    return
                else:
                    print("⚠️ Waiting for recovery")

            if trade_limit_reached and session_profit > 0:

                print("\n🎯 Profit recovered")
                return


while True:

    try:

        asyncio.run(main())

    except websockets.exceptions.ConnectionClosed:

        print("🔌 Connection lost. Reconnecting...")
        time.sleep(5)

    except Exception as e:

        print("❌ Error:", e)
        print("Restarting in 5 seconds...")
        time.sleep(5)
