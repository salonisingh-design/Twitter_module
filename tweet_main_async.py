# import asyncio, aiohttp, time, os
# from config import BEARER_TOKEN, get_output_table
# from cookie_dynamic_handling_async import TokenManager
# from db_manager_async import AsyncDBManager
# from tweet_crawl_aysnc import fetch_tweet_data
#
# CONCURRENCY = 1
# LOG_DIR = os.path.join(os.getcwd(), "logs")  # safer than OneDrive path
# os.makedirs(LOG_DIR, exist_ok=True)
# LOG_FILE = os.path.join(LOG_DIR, "run_log.txt")
#
#
# def log_print(msg: str):
#     """Print to console and also write to file"""
#     print(msg)
#     with open(LOG_FILE, "a", encoding="utf-8") as f:
#         f.write(msg + "\n")
#
#
# async def fetch_and_save(sem, session, token_mgr, db, tweet_url, output_table, input_table):
#     async with sem:
#         tweet_json, status = await fetch_tweet_data(session, token_mgr, tweet_url)
#         await db.insert_output(output_table, tweet_url, tweet_json)
#         await db.update_status(input_table, tweet_url, status)
#         log_print(f"✅ Processed: {tweet_url}")
#
#
# async def main():
#     base_table = "tweets"
#     input_table = "input_tb"
#     output_table = get_output_table(base_table)
#
#     db = await AsyncDBManager.create()
#     await db.create_output_table(output_table)
#     pending_tweets = await db.fetch_pending_tweets(input_table)
#     log_print(f"Found {len(pending_tweets)} pending tweets...")
#
#     token_mgr = TokenManager(BEARER_TOKEN)
#     sem = asyncio.Semaphore(CONCURRENCY)
#
#     async with aiohttp.ClientSession() as session:
#         first_tweet_url = pending_tweets[0]["tweetUrl"] if pending_tweets else None
#
#         total_start = time.time()
#         session_start_time = time.strftime("%Y-%m-%d %X", time.localtime(total_start))
#
#         if first_tweet_url:
#             await token_mgr.refresh_tokens(first_tweet_url)
#
#         tasks = [
#             fetch_and_save(sem, session, token_mgr, db, t["tweetUrl"], output_table, input_table)
#             for t in pending_tweets
#         ]
#         await asyncio.gather(*tasks)
#
#         total_end = time.time()
#         total_duration = round(total_end - total_start, 2)
#         session_end_time = time.strftime("%Y-%m-%d %X", time.localtime(total_end))
#
#     total_end = time.time()
#     total_duration = round(total_end - total_start, 2)
#     session_end_time = time.strftime("%Y-%m-%d %X", time.localtime(total_end))
#
#     summary_text = (
#         f"\n===== 🧾 SESSION SUMMARY ({session_start_time}) =====\n"
#         f"⏱️  Total Duration: {total_duration} seconds\n"
#         f"🔢  Total Responses Processed: {getattr(token_mgr, 'response_count', 'N/A')}\n"
#         f"🕒  Last Token Refresh Duration: {getattr(token_mgr, 'duration', 'N/A')} seconds\n"
#         f"📅  Token Start Time: {time.strftime('%X', time.localtime(getattr(token_mgr, 'start_time', 0))) if getattr(token_mgr, 'start_time', None) else 'N/A'}\n"
#         f"📅  Token End Time:   {time.strftime('%X', time.localtime(getattr(token_mgr, 'end_time', 0))) if getattr(token_mgr, 'end_time', None) else 'N/A'}\n"
#         f"📋  Pending Tweets: {len(pending_tweets)}\n"
#         f"✅  Completed at: {session_end_time}\n"
#         f"{'='*50}\n"
#     )
#
#     log_print(summary_text)
#
#
# if __name__ == "__main__":
#     asyncio.run(main())




import asyncio
import aiohttp
import time
import os
import traceback
from config import BEARER_TOKEN, get_output_table
from cookie_dynamic_handling_async import TokenManager
from db_manager_async import AsyncDBManager
from twitter_module.tweet_crawl_aysnc import fetch_tweet_data
from twitter_module.tweet_parser import parse_tweet_data

CONCURRENCY = 5
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

        except Exception as e:
            log_print(f"❌ Error {tweet_url}: {type(e).__name__} - {e}")
            traceback.print_exc()
            await db.update_status(input_table, tweet_url, "error")


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

        tasks = [
            fetch_parse_save(sem, session, token_mgr, db, t["tweetUrl"], output_table, input_table)
            for t in pending
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
