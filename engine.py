import asyncio
import json
import math
import os
import time
from collections import deque
import aiohttp
import websockets
from dotenv import load_dotenv

# Initialize and pull security configurations from isolated environment
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")
PO_AUTH_STRING = os.getenv("PO_AUTH_STRING", '42["auth",{"session":"YOUR_SESSION_ID","isDemo":1,"uid":12345,"platform":1}]')
PO_COOKIE = os.getenv("PO_COOKIE", "")

# Operational Parameters
PO_WS_URL = "wss://api.po.market/socket.io/?EIO=4&transport=websocket"
TARGET_ASSET = "EURUSD_otc"
WINDOW_SIZE = 30
Z_SCORE_THRESHOLD = 3.3  
COOLDOWN_PERIOD = 20

class PocketOptionEngine:
    def __init__(self):
        self.price_history = deque(maxlen=WINDOW_SIZE)
        self.last_signal_time = 0
        self.http_session = None

    async def init_tg(self):
        """Initializes a persistent aiohttp session for Telegram dispatches."""
        self.http_session = aiohttp.ClientSession()

    async def send_alert(self, msg):
        """Dispatches an asynchronous notification payload directly to your Telegram bot channel."""
        if not self.http_session:
            return
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
        try:
            async with self.http_session.post(url, json=payload) as resp:
                if resp.status != 200:
                    print(f"[-] Telegram API Error response: {await resp.text()}")
        except Exception as e:
            print(f"[-] Network layer error sending Telegram message: {e}")

    def get_z_score(self, current_price):
        """Calculates statistical dispersion profile relative to moving window baseline."""
        if len(self.price_history) < WINDOW_SIZE:
            return 0.0
        
        mean = sum(self.price_history) / len(self.price_history)
        variance = sum((x - mean) ** 2 for x in self.price_history) / len(self.price_history)
        std_dev = math.sqrt(variance)
        
        # Guard clause protecting against flatlines or Zero-Variance constraints
        return (current_price - mean) / std_dev if std_dev > 0 else 0.0

    async def evaluate_tick(self, asset, price):
        """Analyzes pricing velocity, applies statistical filters, and dispatches actions."""
        current_time = time.time()
        z = self.get_z_score(price)
        
        # Capture reference point prior to altering history state array
        last_price = self.price_history[-1] if len(self.price_history) > 0 else None
        self.price_history.append(price)

        # Enforce technical window delay limits
        if current_time - self.last_signal_time < COOLDOWN_PERIOD:
            return

        if abs(z) >= Z_SCORE_THRESHOLD:
            # --- CONSERVATIVE MOMENTUM STALL FILTER ---
            if last_price is not None:
                if z > 0 and price > last_price:
                    return
                if z < 0 and price < last_price:
                    return

            direction = "🔴 PUT (Sell)" if z > 0 else "🟢 CALL (Buy)"
            self.last_signal_time = current_time
            
            alert_msg = (
                f"🚨 *POCKET OPTION OTC SIGNAL* 🚨\n\n"
                f"*Asset:* `{asset.upper()}`\n"
                f"*Action:* {direction}\n"
                f"*Current Spot:* `{price:.5f}`\n"
                f"*Calculated Z-Score:* `{z:.2f}`\n"
                f"*Validation Frame:* 1M - 2M Expiry\n"
                f"⚠️ _Maintain rigid 1% maximum capital safety rule._"
            )
            print(f"[🔥 TRIGGERED] {direction} | Z: {z:.2f} | Spot: {price}")
            await self.send_alert(alert_msg)

    async def handle_heartbeat(self, ws):
        """Keeps WebSocket pipe context active using baseline ping packet structures."""
        try:
            while True:
                await asyncio.sleep(25)  
                await ws.send("2")       
                print("[~] Heartbeat ping sent.")
        except asyncio.CancelledError:
            pass

    def parse_po_message(self, message):
        """Extracts streaming values from serialized Socket.io frames cleanly."""
        if message.startswith("42"):
            try:
                data = json.loads(message[2:])
                event_type = data[0]
                payload = data[1]
                
                if event_type in ["tick", "loadSymbols", "symbolHistory"]:
                    if isinstance(payload, dict) and payload.get("asset") == TARGET_ASSET:
                        return payload["asset"], float(payload["price"])
            except Exception:
                pass
        return None, None

    async def start(self):
        """Kicks off continuous async event evaluation engine."""
        await self.init_tg()
        print(f"[+] Connecting to Pocket Option Feed for {TARGET_ASSET}...")
        
        # Fully authenticated session header masquerade
        handshake_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Origin": "https://po.market",
            "Host": "api.po.market",
            "Cookie": PO_COOKIE
        }
        
        try:
            async for ws in websockets.connect(PO_WS_URL, additional_headers=handshake_headers):
                try:
                    # 1. Complete underlying initial connection frame protocols
                    await ws.send("40") 
                    await asyncio.sleep(1)

                    # 2. Transmit session authorization strings
                    await ws.send(PO_AUTH_STRING)
                    print("[+] Authentication string transmitted successfully.")

                    # 3. Direct streaming target asset focus parameters
                    sub_payload = f'42["changeSymbol", {{"asset": "{TARGET_ASSET}"}}]'
                    await ws.send(sub_payload)
                    print(f"[+] Market data pipe open for {TARGET_ASSET}.")

                    # Fire background heartbeat loop task context
                    heartbeat_task = asyncio.create_task(self.handle_heartbeat(ws))

                    # 4. Stream and loop transaction ticks indefinitely
                    async for message in ws:
                        if message == "3":  # Handle standard incoming engine.io pong frame
                            continue
                            
                        asset, price = self.parse_po_message(message)
                        if price:
                            await self.evaluate_tick(asset, price)

                except websockets.ConnectionClosed:
                    print("[!] Connection severed. Initiating automatic safe retry loop...")
                    heartbeat_task.cancel()
                    await asyncio.sleep(5)
                    continue
                except Exception as e:
                    print(f"[-] Operational runtime failure loop error: {e}")
                    await asyncio.sleep(5)
        finally:
            if self.http_session:
                await self.http_session.close()
                print("[+] Asynchronous HTTP clean teardown phase closed.")

if __name__ == "__main__":
    engine = PocketOptionEngine()
    try:
        asyncio.run(engine.start())
    except KeyboardInterrupt:
        print("\n[-] Operational teardown sequence complete.")