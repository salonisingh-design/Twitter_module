# import json
# import re
# import asyncio
#
#
#
# def clean_text(text):
#     if not isinstance(text, str):
#         return ""
#     try:
#         text = re.sub(r"<.*?>", "", text)  # Remove HTML tags
#         text = text.replace("\\n", " ").replace("\\t", " ").replace("\\r", " ")
#         text = text.replace("\n", " ").replace("\t", " ").replace("\r", " ")
#         text = re.sub(r"\s+", " ", text)  # Remove extra spaces
#         return text.strip()
#     except Exception:
#         return ""
#
#
# def parse_tweet_data(tweet_json):
#     """Safely parse a tweet JSON and return structured data."""
#     if not isinstance(tweet_json, dict) or "data" not in tweet_json:
#         return None
#
#     try:
#         data = tweet_json.get("data", {})
#         tweet_result = data.get("tweetResult", {}).get("result", {})
#         core = tweet_result.get("core", {})
#         user_results = core.get("user_results", {}).get("result", {}).get("core", {})
#         legacy = tweet_result.get("legacy", {})
#         views = tweet_result.get("views", {}).get("count", 0)
#
#         parsed = {
#             "name": user_results.get("name", ""),
#             "screen_name": user_results.get("screen_name", ""),
#             "created_at": user_results.get("created_at", ""),
#             "tweet_id": legacy.get("id_str", ""),
#             "text": clean_text(legacy.get("full_text", "")),
#             "retweet_count": legacy.get("retweet_count", 0),
#             "reply_count": legacy.get("reply_count", 0),
#             "like_count": legacy.get("favorite_count", 0),
#             "quote_count": legacy.get("quote_count", 0),
#             "total_views": views,
#             "bookmark_count": legacy.get("bookmark_count", 0),
#         }
#
#         if not parsed["tweet_id"]:
#             return None  # Skip invalid tweets
#
#         return parsed
#
#     except Exception:
#         return None
#
#
# async def parse_tweet_file(file_path):
#     """Asynchronously read and parse a tweet JSON file."""
#     try:
#         async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
#             content = await f.read()
#             tweet_dict = json.loads(content)
#             parsed = parse_tweet_data(tweet_dict)
#             return parsed
#     except Exception:
#         return None
#
#
# async def process_multiple_tweets(file_paths):
#     """Asynchronously process multiple tweet files concurrently."""
#     tasks = [parse_tweet_file(path) for path in file_paths]
#     results = await asyncio.gather(*tasks, return_exceptions=True)
#     valid_results = [r for r in results if isinstance(r, dict) and r]
#     return valid_results
#
#
# # # --- Example usage ---
# # if __name__ == "__main__":
# #     import glob
# #
# #     async def main():
# #         # Example: read all JSON files in current directory
# #         files = glob.glob("tweets/*.json")  # folder with many tweet files
# #         results = await process_multiple_tweets(files)
# #         print(f"✅ Parsed {len(results)} valid tweets out of {len(files)} files.")
# #         if results:
# #             print(json.dumps(results[:3], indent=2, ensure_ascii=False))  # show sample
# #
# #     asyncio.run(main())


import re
from datetime import datetime

import pytz


def clean_text(text):
    if not isinstance(text, str):
        return ""
    try:
        text = re.sub(r"<.*?>", "", text)  # remove HTML tags
        text = text.replace("\\n", " ").replace("\\t", " ").replace("\\r", " ")
        text = text.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text
    except Exception:
        return ""


def parse_tweet_data(tweet_json):
    if not isinstance(tweet_json, dict):
        return None


    try:
        data = tweet_json.get("data", {})
        tweet_result = data.get("tweetResult", {}).get("result", {})

        core = tweet_result.get("core", {})
        user_data = core.get("user_results", {}).get("result", {}).get("core", {})


        legacy = tweet_result.get("legacy", {})
        views = tweet_result.get("views", {}).get("count", 0)

        post_created = legacy.get("created_at")
        ist_time = None
        if post_created:
            try:

                utc_time = datetime.strptime(post_created, "%a %b %d %H:%M:%S %z %Y")
                ist_time = utc_time.astimezone(pytz.timezone("Asia/Kolkata"))
            except Exception:
                ist_time = None

        retweet_count = legacy.get("retweet_count", 0)
        quote_count = legacy.get("quote_count", 0)

        parsed = {
            "tweet_id": legacy.get("id_str", ""),
            "name": user_data.get("name", ""),
            "screen_name": user_data.get("screen_name", ""),
            "created_at": ist_time.isoformat() if ist_time else None,
            "text": clean_text(legacy.get("full_text", "")),
            "retweet_count": retweet_count,
            "reply_count": legacy.get("reply_count", 0),
            "like_count": legacy.get("favorite_count", 0),
            "quote_count": quote_count,
            "repost_count": retweet_count + quote_count,
            "total_views": views,
            "bookmark_count": legacy.get("bookmark_count", 0),
        }

        if not parsed["tweet_id"]:
            return None

        return parsed

    except Exception as e:
        print(f"⚠️ parse_tweet_data error: {type(e).__name__} - {e}")
        return None