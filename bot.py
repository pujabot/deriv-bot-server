import sys
import json
import asyncio
import websockets
import requests
import traceback
from datetime import datetime, timezone
from collections import deque

# ---------- Helper: log trade ----------
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

# ---------- Shared WebSocket helpers ----------
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

async def buy(ws, amount, ctype, barrier, symbol):
    await send(ws, {
        "proposal": 1,
        "amount": amount,
        "basis": "stake",
        "contract_type": ctype,
        "currency": "USD",
        "duration": 1,
        "duration_unit": "t",
        "symbol": symbol,
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

async def heartbeat(ws):
    while True:
        try:
            await send(ws, {"ping": 1})
            await asyncio.sleep(30)
        except:
            break

# ================= STRATEGY 1: CLASSIC (OVER2 / UNDER6) =================
async def strategy_classic(ws, config, session_profit, trade_count):
    BASE_STAKE = config["baseStake"]
    MARTINGALE_MULT = config["martingaleMult"]
    TAKE_PROFIT = config["takeProfit"]
    STOP_LOSS = config["stopLoss"]
    SERVER_URL = config["serverUrl"]
    USER_ID = config["userId"]
    SESSION_ID = config["sessionId"]
    SYMBOL = "1HZ100V"   # single volatile symbol

    stake = BASE_STAKE
    loss_streak = 0
    mode = "OVER2"

    while True:
        if session_profit >= TAKE_PROFIT:
            print(f"✅ Take profit reached: ${session_profit:.2f}")
            break
        if session_profit <= STOP_LOSS:
            print(f"🛑 Stop loss hit: ${session_profit:.2f}")
            break
        if loss_streak >= 6:
            print("❌ Too many consecutive losses, stopping.")
            break

        if mode == "OVER2":
            cid = await buy(ws, stake, "DIGITOVER", 2, SYMBOL)
            profit = await get_result(ws, cid)
            session_profit += profit
            trade_count += 1
            result_str = "WIN" if profit > 0 else "LOSS"
            await log_trade(SERVER_URL, USER_ID, SESSION_ID, SYMBOL, stake, profit, result_str)
            print(f"📊 Trade {trade_count} [{result_str}]: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake:.2f}")
            if profit > 0:
                stake = BASE_STAKE
                loss_streak = 0
            else:
                stake *= MARTINGALE_MULT
                loss_streak += 1
                mode = "UNDER6"
        elif mode == "UNDER6":
            for _ in range(3):
                cid = await buy(ws, stake, "DIGITUNDER", 6, SYMBOL)
                profit = await get_result(ws, cid)
                session_profit += profit
                trade_count += 1
                result_str = "WIN" if profit > 0 else "LOSS"
                await log_trade(SERVER_URL, USER_ID, SESSION_ID, SYMBOL, stake, profit, result_str)
                print(f"🔄 Recovery trade [{result_str}]: profit ${profit:.2f} | session ${session_profit:.2f}")
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
    return session_profit, trade_count

# ================= STRATEGY 2: SMART SCAN (Volatility + OVER4 Recovery) =================
async def strategy_smart_scan(ws, config, session_profit, trade_count):
    BASE_STAKE = config["baseStake"]
    MARTINGALE_MULT = config["martingaleMult"]
    TAKE_PROFIT = config["takeProfit"]
    STOP_LOSS = config["stopLoss"]
    SERVER_URL = config["serverUrl"]
    USER_ID = config["userId"]
    SESSION_ID = config["sessionId"]
    SYMBOLS = {
        "V10": "1HZ10V",
        "V25": "1HZ25V",
        "V50": "1HZ50V",
        "V75": "1HZ75V",
        "V100": "1HZ100V"
    }

    STUDY_TICKS = 25
    MIN_THRESHOLD = 0.78
    MAX_THRESHOLD = 0.86
    MOMENTUM_TICKS = 5
    PRESSURE_TICKS = 10

    def last_digit(price):
        return int(str(price).replace(".", "")[-1])

    async def get_tick(ws):
        while True:
            msg = await wait_msg(ws, "tick")
            try:
                return msg["tick"]["quote"]
            except:
                continue

    async def study_symbol(ws, symbol):
        await send(ws, {"ticks": symbol, "subscribe": 1})
        digits = []
        while len(digits) < STUDY_TICKS:
            quote = await get_tick(ws)
            digits.append(last_digit(quote))
        await send(ws, {"forget_all": "ticks"})
        over2 = sum(1 for d in digits if d > 2)
        percent = over2 / STUDY_TICKS
        return percent, digits

    def check_momentum(digits):
        last = digits[-MOMENTUM_TICKS:]
        return sum(1 for d in last if d > 2) >= 4

    def digit_pressure(digits):
        sample = digits[-PRESSURE_TICKS:]
        over = sum(1 for d in sample if d > 2)
        under = sum(1 for d in sample if d <= 2)
        return over, under

    async def find_entry(ws):
        print("\n🔍 Scanning Market...")
        while True:
            for name, symbol in SYMBOLS.items():
                percent, digits = await study_symbol(ws, symbol)
                momentum = check_momentum(digits)
                over, under = digit_pressure(digits)
                if MIN_THRESHOLD <= percent <= MAX_THRESHOLD and momentum and over > under:
                    print(f"🚀 ENTRY → {name}")
                    return name, symbol
            print("⏳ No setup → rescanning...\n")
            await asyncio.sleep(1)

    async def smart_recovery(ws, symbol, stake):
        print("🧠 Smart Recovery Mode (OVER 4)")
        recovery_stake = stake * MARTINGALE_MULT
        for i in range(6):
            print(f"🛠 Recovery {i+1} | Stake: {recovery_stake}")
            cid = await buy(ws, recovery_stake, "DIGITOVER", 4, symbol)
            profit = await get_result(ws, cid)
            if profit > 0:
                print("✅ Recovery win\n")
                return profit, recovery_stake
            recovery_stake *= MARTINGALE_MULT
        print("❌ Recovery ended (6 attempts)\n")
        return 0, 0

    stake = BASE_STAKE
    while True:
        if session_profit >= TAKE_PROFIT:
            print(f"✅ Take profit reached: ${session_profit:.2f}")
            break
        if session_profit <= STOP_LOSS:
            print(f"🛑 Stop loss hit: ${session_profit:.2f}")
            break

        name, symbol = await find_entry(ws)
        print(f"\n🎯 Trading on {name}")

        # First trade (OVER 2)
        cid = await buy(ws, stake, "DIGITOVER", 2, symbol)
        profit = await get_result(ws, cid)
        session_profit += profit
        trade_count += 1
        await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake, profit, "WIN" if profit > 0 else "LOSS")
        print(f"📊 Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake}")
        if profit > 0:
            continue

        # Second trade (OVER 2) with increased stake
        stake2 = stake * MARTINGALE_MULT
        cid = await buy(ws, stake2, "DIGITOVER", 2, symbol)
        profit = await get_result(ws, cid)
        session_profit += profit
        trade_count += 1
        await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake2, profit, "WIN" if profit > 0 else "LOSS")
        print(f"📊 Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake2}")
        if profit > 0:
            continue

        # Recovery
        profit, used_stake = await smart_recovery(ws, symbol, stake2)
        session_profit += profit
        if profit != 0:
            trade_count += 1
            await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, used_stake, profit, "WIN" if profit > 0 else "LOSS")
            print(f"🔄 Recovery trade: profit ${profit:.2f} | session ${session_profit:.2f}")

    return session_profit, trade_count

# ================= STRATEGY 3: ADAPTIVE RECOVERY (Dynamic Direction) =================
async def strategy_adaptive_recovery(ws, config, session_profit, trade_count):
    BASE_STAKE = config["baseStake"]
    MARTINGALE_MULT = config["martingaleMult"]
    TAKE_PROFIT = config["takeProfit"]
    STOP_LOSS = config["stopLoss"]
    SERVER_URL = config["serverUrl"]
    USER_ID = config["userId"]
    SESSION_ID = config["sessionId"]
    SYMBOLS = {
        "V10": "1HZ10V",
        "V25": "1HZ25V",
        "V50": "1HZ50V",
        "V75": "1HZ75V",
        "V100": "1HZ100V"
    }

    STUDY_TICKS = 25
    MIN_THRESHOLD = 0.78
    MAX_THRESHOLD = 0.86
    MOMENTUM_TICKS = 5
    PRESSURE_TICKS = 10

    def last_digit(price):
        return int(str(price).replace(".", "")[-1])

    async def get_tick(ws):
        while True:
            msg = await wait_msg(ws, "tick")
            try:
                return msg["tick"]["quote"]
            except:
                continue

    async def study_symbol(ws, symbol):
        await send(ws, {"ticks": symbol, "subscribe": 1})
        digits = []
        while len(digits) < STUDY_TICKS:
            quote = await get_tick(ws)
            digits.append(last_digit(quote))
        await send(ws, {"forget_all": "ticks"})
        over2 = sum(1 for d in digits if d > 2)
        percent = over2 / STUDY_TICKS
        return percent, digits

    def check_momentum(digits):
        last = digits[-MOMENTUM_TICKS:]
        return sum(1 for d in last if d > 2) >= 4

    def digit_pressure(digits):
        sample = digits[-PRESSURE_TICKS:]
        over = sum(1 for d in sample if d > 2)
        under = sum(1 for d in sample if d <= 2)
        return over, under

    async def find_entry(ws):
        print("\n🔍 Scanning Market...")
        while True:
            for name, symbol in SYMBOLS.items():
                percent, digits = await study_symbol(ws, symbol)
                momentum = check_momentum(digits)
                over, under = digit_pressure(digits)
                if MIN_THRESHOLD <= percent <= MAX_THRESHOLD and momentum and over > under:
                    print(f"🚀 ENTRY → {name}")
                    return name, symbol
            print("⏳ No setup → rescanning...\n")
            await asyncio.sleep(1)

    async def smart_recovery(ws, symbol, stake):
        print("🧠 Smart Recovery Mode (Dynamic)")
        recovery_stake = stake * MARTINGALE_MULT
        for i in range(6):
            _, digits = await study_symbol(ws, symbol)
            over, under = digit_pressure(digits)
            if over > under:
                ctype, barrier = "DIGITOVER", 5
                print("📈 Market bullish → OVER 5")
            else:
                ctype, barrier = "DIGITUNDER", 4
                print("📉 Market bearish → UNDER 4")
            print(f"🛠 Recovery {i+1} | Stake: {recovery_stake}")
            cid = await buy(ws, recovery_stake, ctype, barrier, symbol)
            profit = await get_result(ws, cid)
            if profit > 0:
                print("✅ Recovery win\n")
                return profit, recovery_stake
            recovery_stake *= MARTINGALE_MULT
        print("❌ Recovery ended (6 attempts)\n")
        return 0, 0

    stake = BASE_STAKE
    while True:
        if session_profit >= TAKE_PROFIT:
            print(f"✅ Take profit reached: ${session_profit:.2f}")
            break
        if session_profit <= STOP_LOSS:
            print(f"🛑 Stop loss hit: ${session_profit:.2f}")
            break

        name, symbol = await find_entry(ws)
        print(f"\n🎯 Trading on {name}")

        # First trade (OVER 2)
        cid = await buy(ws, stake, "DIGITOVER", 2, symbol)
        profit = await get_result(ws, cid)
        session_profit += profit
        trade_count += 1
        await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake, profit, "WIN" if profit > 0 else "LOSS")
        print(f"📊 Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake}")
        if profit > 0:
            continue

        # Second trade (OVER 2) with increased stake
        stake2 = stake * MARTINGALE_MULT
        cid = await buy(ws, stake2, "DIGITOVER", 2, symbol)
        profit = await get_result(ws, cid)
        session_profit += profit
        trade_count += 1
        await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake2, profit, "WIN" if profit > 0 else "LOSS")
        print(f"📊 Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake2}")
        if profit > 0:
            continue

        # Recovery
        profit, used_stake = await smart_recovery(ws, symbol, stake2)
        session_profit += profit
        if profit != 0:
            trade_count += 1
            await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, used_stake, profit, "WIN" if profit > 0 else "LOSS")
            print(f"🔄 Recovery trade: profit ${profit:.2f} | session ${session_profit:.2f}")

    return session_profit, trade_count

# ================= STRATEGY 4: SIMPLE OVER2 (High‑Low Stakes) =================
async def strategy_simple_over2(ws, config, session_profit, trade_count):
    BASE_STAKE = config["baseStake"]
    MARTINGALE_MULT = config["martingaleMult"]
    TAKE_PROFIT = config["takeProfit"]
    STOP_LOSS = config["stopLoss"]
    SERVER_URL = config["serverUrl"]
    USER_ID = config["userId"]
    SESSION_ID = config["sessionId"]
    SYMBOLS = {
        "V10": "1HZ10V",
        "V25": "1HZ25V",
        "V50": "1HZ50V",
        "V75": "1HZ75V",
        "V100": "1HZ100V"
    }

    STUDY_TICKS = 25
    MIN_THRESHOLD = 0.78
    MAX_THRESHOLD = 0.86
    MOMENTUM_TICKS = 5
    PRESSURE_TICKS = 10

    def last_digit(price):
        return int(str(price).replace(".", "")[-1])

    async def get_tick(ws):
        while True:
            msg = await wait_msg(ws, "tick")
            try:
                return msg["tick"]["quote"]
            except:
                continue

    async def study_symbol(ws, symbol):
        await send(ws, {"ticks": symbol, "subscribe": 1})
        digits = []
        while len(digits) < STUDY_TICKS:
            quote = await get_tick(ws)
            digits.append(last_digit(quote))
        await send(ws, {"forget_all": "ticks"})
        over2 = sum(1 for d in digits if d > 2)
        percent = over2 / STUDY_TICKS
        return percent, digits

    def check_momentum(digits):
        last = digits[-MOMENTUM_TICKS:]
        return sum(1 for d in last if d > 2) >= 4

    def digit_pressure(digits):
        sample = digits[-PRESSURE_TICKS:]
        over = sum(1 for d in sample if d > 2)
        under = sum(1 for d in sample if d <= 2)
        return over, under

    async def find_entry(ws):
        print("\n🔍 Scanning Market...")
        while True:
            for name, symbol in SYMBOLS.items():
                percent, digits = await study_symbol(ws, symbol)
                momentum = check_momentum(digits)
                over, under = digit_pressure(digits)
                if MIN_THRESHOLD <= percent <= MAX_THRESHOLD and momentum and over > under:
                    print(f"🚀 ENTRY → {name}")
                    return name, symbol
            print("⏳ No setup → rescanning...\n")
            await asyncio.sleep(1)

    stake = BASE_STAKE
    stake2 = stake * 0.1   # second stake is small (custom)
    while True:
        if session_profit >= TAKE_PROFIT:
            print(f"✅ Take profit reached: ${session_profit:.2f}")
            break
        if session_profit <= STOP_LOSS:
            print(f"🛑 Stop loss hit: ${session_profit:.2f}")
            break

        name, symbol = await find_entry(ws)
        print(f"\n🎯 Trading on {name}")

        # First trade (OVER 2)
        cid = await buy(ws, stake, "DIGITOVER", 2, symbol)
        profit = await get_result(ws, cid)
        session_profit += profit
        trade_count += 1
        await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake, profit, "WIN" if profit > 0 else "LOSS")
        print(f"📊 Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake}")
        if profit > 0:
            continue

        # Second trade (OVER 2) with smaller stake
        cid = await buy(ws, stake2, "DIGITOVER", 2, symbol)
        profit = await get_result(ws, cid)
        session_profit += profit
        trade_count += 1
        await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake2, profit, "WIN" if profit > 0 else "LOSS")
        print(f"📊 Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake2}")
        if profit > 0:
            continue

        # Recovery (increase stake each time)
        recovery_stake = stake2 * MARTINGALE_MULT
        for i in range(6):
            print(f"🛠 Recovery {i+1} | Stake: {recovery_stake}")
            cid = await buy(ws, recovery_stake, "DIGITOVER", 2, symbol)
            profit = await get_result(ws, cid)
            session_profit += profit
            trade_count += 1
            await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, recovery_stake, profit, "WIN" if profit > 0 else "LOSS")
            if profit > 0:
                print("✅ Recovery win\n")
                break
            recovery_stake *= MARTINGALE_MULT

    return session_profit, trade_count

# ================= STRATEGY 5: SMART SCAN ENHANCED (OVER5 Recovery) =================
async def strategy_smart_scan_enhanced(ws, config, session_profit, trade_count):
    BASE_STAKE = config["baseStake"]
    MARTINGALE_MULT = config["martingaleMult"]
    TAKE_PROFIT = config["takeProfit"]
    STOP_LOSS = config["stopLoss"]
    SERVER_URL = config["serverUrl"]
    USER_ID = config["userId"]
    SESSION_ID = config["sessionId"]
    SYMBOLS = {
        "V10": "1HZ10V",
        "V25": "1HZ25V",
        "V50": "1HZ50V",
        "V75": "1HZ75V",
        "V100": "1HZ100V"
    }

    # Strategy-specific parameters (as in your code)
    STUDY_TICKS = 25
    MIN_THRESHOLD = 0.76
    MAX_THRESHOLD = 0.90
    MOMENTUM_TICKS = 4
    PRESSURE_TICKS = 6

    def last_digit(price):
        return int(str(price).replace(".", "")[-1])

    async def get_tick(ws):
        while True:
            msg = await wait_msg(ws, "tick")
            try:
                return msg["tick"]["quote"]
            except:
                continue

    async def study_symbol(ws, symbol):
        await send(ws, {"ticks": symbol, "subscribe": 1})
        digits = []
        while len(digits) < STUDY_TICKS:
            quote = await get_tick(ws)
            digits.append(last_digit(quote))
        await send(ws, {"forget_all": "ticks"})
        over2 = sum(1 for d in digits if d > 2)
        percent = over2 / STUDY_TICKS
        return percent, digits

    def check_momentum(digits):
        last = digits[-MOMENTUM_TICKS:]
        return sum(1 for d in last if d > 2) >= 2   # weaker momentum

    def digit_pressure(digits):
        sample = digits[-PRESSURE_TICKS:]
        over = sum(1 for d in sample if d > 4)   # pressure based on >4
        under = sum(1 for d in sample if d <= 4)
        return over, under

    async def find_entry(ws):
        print("\n🔍 Scanning Market (Enhanced)...")
        while True:
            for name, symbol in SYMBOLS.items():
                percent, digits = await study_symbol(ws, symbol)
                momentum = check_momentum(digits)
                over, under = digit_pressure(digits)
                print("\n" + "="*40)
                print(f"📊 SYMBOL: {name}")
                print(f"📈 Strength: {percent*100:.2f}%")
                print(f"⚡ Momentum: {'✅' if momentum else '❌'}")
                print(f"🔥 >4 Pressure: {over}")
                print(f"❄️ ≤4 Pressure: {under}")
                print("="*40)
                if MIN_THRESHOLD <= percent <= MAX_THRESHOLD and momentum and over > under:
                    print(f"🚀 ENTRY → {name}")
                    return name, symbol
            print("⏳ No setup → rescanning...\n")
            await asyncio.sleep(1)

    stake = BASE_STAKE
    while True:
        if session_profit >= TAKE_PROFIT:
            print(f"✅ Take profit reached: ${session_profit:.2f}")
            break
        if session_profit <= STOP_LOSS:
            print(f"🛑 Stop loss hit: ${session_profit:.2f}")
            break

        name, symbol = await find_entry(ws)
        print(f"\n🎯 Trading on {name}")

        # FIRST TRADE (OVER 2)
        cid = await buy(ws, stake, "DIGITOVER", 2, symbol)
        profit = await get_result(ws, cid)
        session_profit += profit
        trade_count += 1
        await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake, profit, "WIN" if profit > 0 else "LOSS")
        print(f"📊 Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake}")
        if profit > 0:
            continue

        # SECOND TRADE (OVER 5) – note: barrier 5
        stake2 = stake  # as per your SECOND_STAKE = FIRST_STAKE
        cid = await buy(ws, stake2, "DIGITOVER", 5, symbol)
        profit = await get_result(ws, cid)
        session_profit += profit
        trade_count += 1
        await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, stake2, profit, "WIN" if profit > 0 else "LOSS")
        print(f"📊 Trade {trade_count}: profit ${profit:.2f} | session ${session_profit:.2f} | stake ${stake2}")
        if profit > 0:
            continue

        # RECOVERY (OVER 5) with increasing stake
        recovery_stake = stake2 * MARTINGALE_MULT
        print("🧠 Smart Recovery Mode (OVER 5)")
        for i in range(8):
            print(f"🛠 Recovery {i+1} | Stake: {recovery_stake}")
            cid = await buy(ws, recovery_stake, "DIGITOVER", 5, symbol)
            profit = await get_result(ws, cid)
            session_profit += profit
            trade_count += 1
            await log_trade(SERVER_URL, USER_ID, SESSION_ID, symbol, recovery_stake, profit, "WIN" if profit > 0 else "LOSS")
            if profit > 0:
                print("✅ Recovery win\n")
                break
            recovery_stake *= MARTINGALE_MULT

    return session_profit, trade_count

# ================= MAIN WRAPPER WITH STRATEGY SELECTION =================
async def run_session(config):
    TOKEN = config["token"]
    USER_ID = config["userId"]
    SESSION_ID = config["sessionId"]
    BASE_STAKE = float(config["baseStake"])
    MARTINGALE_MULT = float(config["martingaleMult"])
    TAKE_PROFIT = float(config["takeProfit"])
    STOP_LOSS = float(config["stopLoss"])
    SERVER_URL = config["serverUrl"]
    STRATEGY = config.get("strategy", "classic")

    if STOP_LOSS >= 0:
        STOP_LOSS = -5.0
        print(f"⚠️ Stop loss was set to non‑negative, changed to -5.0", file=sys.stderr)

    WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id=1089"

    async with websockets.connect(WS_URL) as ws:
        await send(ws, {"authorize": TOKEN})
        auth_msg = await wait_msg(ws, "authorize")
        if "error" in auth_msg:
            print(f"Authorization failed: {auth_msg}", file=sys.stderr)
            return
        print(f"✅ Authorized successfully (user {USER_ID[:6]})")

        # Start heartbeat
        asyncio.create_task(heartbeat(ws))

        # Run selected strategy
        session_profit = 0.0
        trade_count = 0
        if STRATEGY == "classic":
            session_profit, trade_count = await strategy_classic(ws, config, session_profit, trade_count)
        elif STRATEGY == "smart_scan":
            session_profit, trade_count = await strategy_smart_scan(ws, config, session_profit, trade_count)
        elif STRATEGY == "adaptive_recovery":
            session_profit, trade_count = await strategy_adaptive_recovery(ws, config, session_profit, trade_count)
        elif STRATEGY == "simple_over2":
            session_profit, trade_count = await strategy_simple_over2(ws, config, session_profit, trade_count)
        elif STRATEGY == "smart_scan_enhanced":
            session_profit, trade_count = await strategy_smart_scan_enhanced(ws, config, session_profit, trade_count)
        else:
            print(f"Unknown strategy: {STRATEGY}", file=sys.stderr)
            return

        print(f"🏁 Session finished. Final profit: ${session_profit:.2f}")

# ---------- Auto‑reconnect wrapper ----------
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
            break
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
