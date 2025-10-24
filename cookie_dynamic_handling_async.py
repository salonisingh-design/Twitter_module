import re
import aiohttp, asyncio, requests, time, random
from typing import Optional, Dict
from bs4 import BeautifulSoup


def fetch_cookie_data(tweet_url: str) -> Dict[str, Optional[str]]:

    time.sleep(random.uniform(0.1, 0.3))
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
        r = requests.get(tweet_url, headers=headers, timeout=10)
        if r.status_code == 200:
            text = r.text
            soup = BeautifulSoup(text, "html.parser")

            for script in soup.find_all("script"):
                if not script.string:
                    continue
                script_text = script.string

                # guest_id_marketing
                match_marketing = re.search(r'guest_id_marketing=([^";]+)', script_text)
                if match_marketing:
                    result["guest_id_marketing"] = match_marketing.group(1)

                # guest_id_ads
                match_ads = re.search(r'guest_id_ads=([^";]+)', script_text)
                if match_ads:
                    result["guest_id_ads"] = match_ads.group(1)

                # personalization_id
                match_personalization = re.search(
                    r'personalization_id\\?=\\?"?([^"\\;]+)', script_text
                )
                if match_personalization:
                    result["personalization_id"] = match_personalization.group(1)

            return result
        else:
            print(f"⚠️ Failed to fetch cookie data (status {r.status_code})")

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

    async def _fetch_guest_token(self) -> str:
        url = "https://api.twitter.com/1.1/guest/activate.json"
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',"
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
            print("🔄 Refreshing tokens...")
            self.guest_token = await self._fetch_guest_token()

            cookie_data = await asyncio.to_thread(fetch_cookie_data, tweet_url)
            self.guest_id_marketing = cookie_data.get("guest_id_marketing")
            self.personalization_id = cookie_data.get("personalization_id")

            print("✅ Tokens refreshed")
            print("Guest Token:", self.guest_token)
            print("Guest ID Marketing:", self.guest_id_marketing)
            print("Personalization ID:", self.personalization_id)

    async def get_headers(self) -> dict:
        if not self.guest_token or not self.guest_id_marketing:
            raise RuntimeError("Tokens not initialized. Call refresh_tokens() first.")
        headers = {
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
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        }
        # print(headers)
        return headers

    async def ensure_valid_tokens(self, tweet_url: str, response_status: int):
        if response_status != 200:
            print(f"⚠️ Response status {response_status}, refreshing tokens...")
            await self.refresh_tokens(tweet_url)


# ----------------------- Example Usage -----------------------
# import asyncio
#
# async def main():
#     bearer_token = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
#     tweet_url = "https://x.com/dandaneshvar/status/1674395155892260865"  # Example tweet
#
#     token_manager = TokenManager(bearer_token)
#     await token_manager.refresh_tokens(tweet_url)
#
#     headers = await token_manager.get_headers()
#     print("\n✅ Final Headers with Personalization ID:")
#     for k, v in headers.items():
#         print(f"{k}: {v}")
#
# asyncio.run(main())
