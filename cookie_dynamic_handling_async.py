import re
import aiohttp, asyncio, requests, time, random
from typing import Optional, Dict
from bs4 import BeautifulSoup
from proxy_detail import cookie_proxy_info

def fetch_cookie_data(tweet_url: str) -> Dict[str, Optional[str]]:
    time.sleep(random.uniform(2, 3))
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    }

    result = {
        "guest_id_marketing": None,
        "guest_id_ads": None,
        "personalization_id": None
    }

    try:
        r = requests.get(tweet_url, headers=headers, timeout=10, proxies=cookie_proxy_info())
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for script in soup.find_all("script"):
                if not script.string:
                    continue
                script_text = script.string
                match_marketing = re.search(r'guest_id_marketing=([^";]+)', script_text)
                match_ads = re.search(r'guest_id_ads=([^";]+)', script_text)
                match_personalization = re.search(r'personalization_id\\?=\\?"?([^"\\;]+)', script_text)
                if match_marketing:
                    result["guest_id_marketing"] = match_marketing.group(1)
                if match_ads:
                    result["guest_id_ads"] = match_ads.group(1)
                if match_personalization:
                    result["personalization_id"] = match_personalization.group(1)
            return result
    except Exception as e:
        print("Cookie fetch issue:", e)
    return result


class TokenManager:
    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token
        self.guest_token: Optional[str] = None
        self.guest_id_marketing: Optional[str] = None
        self.personalization_id: Optional[str] = None
        self.lock = asyncio.Lock()

        # New: Track token usage to rotate on rate limits
        self.last_refresh_time = 0
        self.min_refresh_interval = 10  # seconds

    async def _fetch_guest_token(self) -> str:
        url = "https://api.twitter.com/1.1/guest/activate.json"
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
                token = data.get("guest_token")
                if not token:
                    raise RuntimeError("Failed to get guest_token")
                return token

    async def refresh_tokens(self, tweet_url: str):
        async with self.lock:
            now = time.time()
            if now - self.last_refresh_time < self.min_refresh_interval:
                # Avoid refreshing too frequently
                return
            print("🔄 Refreshing tokens...")
            self.guest_token = await self._fetch_guest_token()
            cookie_data = await asyncio.to_thread(fetch_cookie_data, tweet_url)
            self.guest_id_marketing = cookie_data.get("guest_id_marketing")
            self.personalization_id = cookie_data.get("personalization_id")
            self.last_refresh_time = time.time()
            print("✅ Tokens refreshed")

    async def get_headers(self) -> dict:
        if not self.guest_token or not self.guest_id_marketing:
            raise RuntimeError("Tokens not initialized. Call refresh_tokens() first.")
        return {
            "Authorization": f"Bearer {self.bearer_token}",
            "X-Guest-Token": self.guest_token,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            'Cookie': (
                f'guest_id_marketing={self.guest_id_marketing}; '
                f'guest_id_ads={self.guest_id_marketing}; '
                f'guest_id={self.guest_id_marketing}; '
                f'gt={self.guest_token}; '
                f'personalization_id="{self.personalization_id}"'
            ),
            'origin': 'https://x.com',
            'priority': 'u=1, i:',
            'referer': 'https://x.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        }

    async def ensure_valid_tokens(self, tweet_url: str, response_status: int):
        """
        Rotate tokens automatically if a rate-limit (429) or authorization error occurs.
        """
        if response_status in (401, 403, 429):
            print(f"⚠️ Response {response_status}, rotating tokens...")
            await self.refresh_tokens(tweet_url)
