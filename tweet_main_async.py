import asyncio
import aiohttp
import time
import os
import traceback
from config import BEARER_TOKEN, get_output_table
from cookie_dynamic_handling_async import TokenManager
from db_manager_async import AsyncDBManager
from tweet_crawl_aysnc import fetch_tweet_data
from tweet_parser import parse_tweet_data
import random

CONCURRENCY = 10
BATCH_SIZE = 500  # Process tweets in batches
MAX_RETRIES = 3
LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "run_log.txt")


def log_print(msg: str):
    ts = time.strftime("%Y-%m-%d %X", time.localtime())
    formatted = f"[{ts}] {msg}"
    print(formatted)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(formatted + "\n")


async def fetch_parse_save(sem, session, token_mgr, db, tweet_url, output_table, input_table):
    async with sem:
        await asyncio.sleep(random.uniform(0.1, 0.3))  # jitter to reduce rate-limit
        status = "unknown"
        for attempt in range(MAX_RETRIES):
            try:
                tweet_json, status = await fetch_tweet_data(session, token_mgr, tweet_url)
                parsed_tweet = parse_tweet_data(tweet_json) if tweet_json else None

                if parsed_tweet:
                    await db.insert_parsed_tweet(output_table, tweet_url, parsed_tweet, tweet_json)
                else:
                    log_print(f"⚠️ No valid data parsed for {tweet_url}")
                    status = "parse_failed"

                await db.update_status(input_table, tweet_url, status)
                log_print(f"✅ {tweet_url} — {status}")
                break  # success, exit retry loop
            except Exception as e:
                log_print(f"❌ Attempt {attempt+1} failed for {tweet_url}: {type(e).__name__} - {e}")
                traceback.print_exc()
                await asyncio.sleep(2 ** attempt * random.uniform(0.5, 1.5))  # exponential backoff
        else:
            # All retries failed
            status = "error"
            await db.update_status(input_table, tweet_url, status)
            log_print(f"❌ {tweet_url} failed after {MAX_RETRIES} retries.")


async def main():
    base_table = "tweets"
    input_table = "input_tb"
    output_table = get_output_table(base_table)

    db = await AsyncDBManager.create()
    await db.create_output_table(output_table)

    pending = await db.fetch_pending_tweets(input_table)
    log_print(f"📋 Found {len(pending)} pending tweets.")

    if not pending:
        await db.close()
        return

    token_mgr = TokenManager(BEARER_TOKEN)
    sem = asyncio.Semaphore(CONCURRENCY)
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        first_url = pending[0]["tweetUrl"]
        await token_mgr.refresh_tokens(first_url)

        # Process tweets in batches
        for i in range(0, len(pending), BATCH_SIZE):
            batch = pending[i:i + BATCH_SIZE]
            tasks = [
                fetch_parse_save(sem, session, token_mgr, db, t["tweetUrl"], output_table, input_table)
                for t in batch
            ]
            await asyncio.gather(*tasks)

    await db.close()

    duration = round(time.time() - start_time, 2)
    log_print(f"\n===== 🧾 SESSION SUMMARY =====\n"
              f"⏱️ Duration: {duration}s\n"
              f"📋 Tweets processed: {len(pending)}\n"
              f"====================================\n")


if __name__ == "__main__":
    asyncio.run(main())
