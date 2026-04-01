import sys
import json
import asyncio
import websockets
import requests
import traceback
from datetime import datetime, timezone
from collections import deque

# ---------- Helper: log trade to Flask server ----------
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
                "raw_time": datetime.now(timezone.utc).isoformat()
            },
            timeout=5
        )
    except Exception as e:
        print(f"Logging failed: {e}", file=sys.stderr)

# ---------- Strategy helpers ----------
def last_digit(price):
    """Return the last digit of a price (ignoring decimal point)."""
    return int(str(price).replace(".", "")[-1])

def check_momentum(digits):
    """Return True if at least 4 of the last MOMENTUM_TICKS digits are >2."""
    MOMENTUM_TICKS = 5
    last = digits[-MOMENTUM_TICKS:]
    return sum(1 for d in last if d > 2) >= 4

def digit_pressure(digits):
    """Return (count_over2, count_under2) for the last PRESSURE_TICKS ticks."""
    PRESSURE_TICKS = 10
    sample = digits[-PRESSURE_TICKS:]
    over = sum(1 for d in sample if d > 2)
    under = sum(1 for d in sample if d <= 2)
    return over, under

def dashboard(name, percent, momentum, over, under):
    """Print a nice dashboard of the current symbol analysis."""
    print("\n" + "="*40)
    print(f"📊 SYMBOL: {name}")
    print(f"📈 Strength: {percent*100:.2f}%")
    print(f"⚡ Momentum: {'✅' if momentum else '❌'}")
    print(f"🔥 >2 Pressure: {over}")
    print(f"❄️ ≤2 Pressure: {under}")
    print("="*40)

# ---------- Main bot logic (runs a single session) ----------
async def run_session(config):
    # ----- Read configuration -----
    TOKEN = config["token"]
    USER_ID = config["userId"]
    SESSION_ID = config["sessionId"]
    BASE_STAKE = float(config["baseStake"])
    MARTINGALE_MULT = float(config["martingaleMult"])
    TAKE_PROFIT = float(config["takeProfit"])
    STOP_LOSS = float(config["stopLoss"])
    SERVER_URL = config["serverUrl"]

    # Ensure stop loss is negative
    if STOP_LOSS >= 0:
        STOP_LOSS = -5.0
        print(f"⚠️ Stop loss was set to non‑negative, changed to -5.0", file=sys.stderr)

    # ----- Constants -----
    APP_ID = 1089
    WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
    CURRENCY = "USD"
    DURATION = 1
    DURATION_UNIT = "t"

    # Symbol list (same as your strategy)
    SYMBOLS = {
        "V10": "1HZ10V",
        "V25": "1HZ25V",
        "V50": "1HZ50V",
        "V75": "1HZ75V",
        "V100": "1HZ100V"
    }

    # Strategy parameters derived from settings
    FIRST_STAKE = BASE_STAKE
    SECOND_STAKE = BASE_STAKE * 2          # You can adjust this factor later
    RECOVERY_MULTIPLIER = MARTINGALE_MULT   # recovery stake multiplier
    RECOVERY_STEPS = 6                     # max recovery attempts

    # ----- WebSocket helpers (shared with the strategy) -----
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
            "symbol": symbol,          # symbol is captured from outer scope
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

    # ----- Strategy-specific functions (use the helpers above) -----
    async def study_symbol(ws, sym):
        """Collect STUDY_TICKS ticks and return (over2_percentage, digits_list)."""
        STUDY_TICKS = 25
        await send(ws, {"ticks": sym, "subscribe": 1})
        digits = []
        while len(digits) < STUDY_TICKS:
            tick = await wait_msg(ws, "tick")
            digits.append(last_digit(tick["tick"]["quote"]))
        await send(ws, {"forget_all": "ticks"})
        over2 = sum(1 for d in digits if d > 2)
        percent = over2 / STUDY_TICKS
        return percent, digits

    async def find_entry(ws):
        """Scan symbols and return (name, symbol) when a good setup is found."""
        print("\n🔍 Scanning Market...")
        while True:
            for name, sym in SYMBOLS.items():
                percent, digits = await study_symbol(ws, sym)
                momentum = check_momentum(digits)
                over, under = digit_pressure(digits)
                dashboard(name, percent, momentum, over, under)

                # Entry condition: strength between thresholds, momentum strong, >2 pressure > ≤2 pressure
                MIN_THRESHOLD = 0.78
                MAX_THRESHOLD = 0.86
                if MIN_THRESHOLD <= percent <= MAX_THRESHOLD and momentum and over > under:
                    print(f"🚀 ENTRY → {name}")
                    return name, sym
            print("⏳ No setup → rescanning...\n")

    async def smart_recovery(ws, symbol):
        """Attempt up to RECOVERY_STEPS trades with increasing stake (OVER 4)."""
        print("🧠 Smart Recovery Mode (OVER 4)")
        stake = SECOND_STAKE * RECOVERY_MULTIPLIER
        for i in range(RECOVERY_STEPS):
            print(f"🛠 Recovery {i+1} | Stake: {stake}")
            # Use the existing buy and get_result functions, but with symbol from outer scope
            cid = await buy(ws, stake, "DIGITOVER", 4)
            profit = await get_result(ws, cid)
            if profit > 0:
                print("✅ Recovery win\n")
                return profit, stake
            stake *= RECOVERY_MULTIPLIER
        print("❌ Recovery ended (no win after all attempts)\n")
        return 0, 0   # no profit

    # ----- Connect and start trading -----
    async with websockets.connect(WS_URL) as ws:
        # Authorize
        await send(ws, {"authorize": TOKEN})
        auth_msg = await wait_msg(ws, "authorize")
        if "error" in auth_msg:
            print(f"Authorization failed: {auth_msg}", file=sys.stderr)
            return
        print(f"✅ Authorized successfully (user {USER_ID[:6]})")

        # Start heartbeat task
        asyncio.create_task(heartbeat(ws))

        session_profit = 0.0
        trade_count = 0

        # ----- Main trading loop (new strategy) -----
        while True:
            # Check global profit targets
            if session_profit >= TAKE_PROFIT:
                print(f"✅ Take profit reached: ${session_profit:.2f}")
                break
            if session_profit <= STOP_LOSS:
                print(f"🛑 Stop loss hit: ${session_profit:.2f}")
                break

            # 1. Find a symbol that meets the entry conditions
            name, symbol = await find_entry(ws)
            print(f"\n🎯 Trading on {name}")

            # 2. First trade (OVER 2) with FIRST_STAKE
            cid = await buy(ws, FIRST_STAKE, "DIGITOVER", 2)
            profit = await get_result(ws, cid)
            session_profit += profit
            trade_count += 1
            result_str = "WIN" if profit > 0 else "LOSS"
            await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, FIRST_STAKE, profit, result_str)
            print(f"📊 Trade {trade_count} [{result_str}]: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${FIRST_STAKE}")
            if profit > 0:
                continue   # profit, restart cycle

            # 3. Second trade (OVER 2) with SECOND_STAKE
            cid = await buy(ws, SECOND_STAKE, "DIGITOVER", 2)
            profit = await get_result(ws, cid)
            session_profit += profit
            trade_count += 1
            result_str = "WIN" if profit > 0 else "LOSS"
            await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, SECOND_STAKE, profit, result_str)
            print(f"📊 Trade {trade_count} [{result_str}]: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${SECOND_STAKE}")
            if profit > 0:
                continue   # profit, restart cycle

            # 4. Smart recovery (OVER 4 with increasing stakes)
            profit, stake_used = await smart_recovery(ws, symbol)
            session_profit += profit
            trade_count += 1
            result_str = "WIN" if profit > 0 else "LOSS"
            await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake_used, profit, result_str)
            print(f"🔄 Recovery trade [{result_str}]: profit ${profit:.2f} | session ${session_profit:.2f}")

            # Continue the cycle (will re‑scan for a new entry)

        print(f"🏁 Session finished. Final profit: ${session_profit:.2f}")

# ---------- Heartbeat ----------
async def heartbeat(ws):
    while True:
        try:
            print("❤️ Bot Alive")
            await send(ws, {"ping": 1})
            await asyncio.sleep(30)
        except:
            break

# ---------- Auto‑reconnect wrapper (unchanged) ----------
async def main():
    config_str = sys.stdin.read()
    if not config_str:
        print("No config received", file=sys.stderr)
        return
    config = json.loads(config_str)

    while True:
        try:
            await run_session(config)
            print("Bot session completed normally.")
            break  # exit if session finished cleanly (take profit / stop loss / max losses)
        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                ConnectionResetError) as e:
            print(f"🔌 Connection lost ({e}). Reconnecting in 5 seconds...", file=sys.stderr)
            await asyncio.sleep(5)
        except Exception as e:
            print(f"💥 Unexpected error: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            print("Restarting in 10 seconds...", file=sys.stderr)
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
