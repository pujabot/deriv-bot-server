import sys
import asyncio
import websockets
import json

async def get_balance(token):
    WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    try:
        async with websockets.connect(WS_URL) as ws:
            await ws.send(json.dumps({"authorize": token}))
            auth_resp = await ws.recv()
            auth_data = json.loads(auth_resp)
            if "error" in auth_data:
                return -1
            await ws.send(json.dumps({"balance": 1}))
            bal_resp = await ws.recv()
            bal_data = json.loads(bal_resp)
            return bal_data.get("balance", {}).get("balance", 0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return -1

if __name__ == "__main__":
    token = sys.argv[1]
    balance = asyncio.run(get_balance(token))
    print(balance)
