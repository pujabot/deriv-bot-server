import sys
import asyncio
import websockets
import json

async def get_balance_and_type(token):
    WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    try:
        async with websockets.connect(WS_URL) as ws:
            await ws.send(json.dumps({"authorize": token}))
            auth_resp = await ws.recv()
            auth_data = json.loads(auth_resp)
            if "error" in auth_data:
                return None, None
            loginid = auth_data["authorize"]["loginid"]
            await ws.send(json.dumps({"balance": 1}))
            bal_resp = await ws.recv()
            bal_data = json.loads(bal_resp)
            balance = bal_data.get("balance", {}).get("balance", 0)
            return balance, loginid
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return None, None

if __name__ == "__main__":
    token = sys.argv[1]
    bal, lid = asyncio.run(get_balance_and_type(token))
    if bal is None:
        print(-1)
    else:
        # Output JSON: {"balance": x, "loginid": "VRTC..."}
        result = {"balance": bal, "loginid": lid}
        print(json.dumps(result))
