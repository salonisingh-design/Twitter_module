# import asyncpg
# import json
# from config import POSTGRES_CONFIG
#
#
# class AsyncDBManager:
#     def __init__(self, pool: asyncpg.Pool):
#         self.pool = pool
#
#     @classmethod
#     async def create(cls):
#         pool = await asyncpg.create_pool(**POSTGRES_CONFIG)
#         return cls(pool)
#
#     async def create_output_table(self, table_name: str):
#         async with self.pool.acquire() as conn:
#             await conn.execute(f"""
#             CREATE TABLE IF NOT EXISTS "Twitter"."{table_name}" (
#                 id SERIAL PRIMARY KEY,
#                 tweet_url TEXT UNIQUE,
#                 tweet_json JSONB,
#                 created_at TIMESTAMP DEFAULT NOW()
#             );
#             """)
#             print(f"✅ Output table '{table_name}' ensured.")
#
#     async def fetch_pending_tweets(self, input_table: str):
#         async with self.pool.acquire() as conn:
#             try:
#                 rows = await conn.fetch(
#                     f'SELECT * FROM "Twitter"."{input_table}" WHERE status = $1', 'pending'
#                 )
#                 return rows
#             except Exception as e:
#                 print(f"❌ Error fetching pending tweets: {type(e).__name__} - {e}")
#                 raise
#
#     async def update_status(self, input_table: str, tweet_url: str, status: str):
#         async with self.pool.acquire() as conn:
#             try:
#                 await conn.execute(
#                     f'UPDATE "Twitter"."{input_table}" SET status = $1 WHERE "tweetUrl" = $2',
#                     status, tweet_url
#                 )
#                 print(f"✅ Updated status for {tweet_url} to {status}")
#             except Exception as e:
#                 print(f"❌ Error updating status for {tweet_url}: {type(e).__name__} - {e}")
#                 raise
#
#     async def insert_output(self, table_name: str, tweet_url: str, tweet_json: dict):
#         async with self.pool.acquire() as conn:
#             try:
#                 await conn.execute(
#                     f'''
#                     INSERT INTO "Twitter"."{table_name}" (tweet_url, tweet_json)
#                     VALUES ($1, $2)
#                     ON CONFLICT (tweet_url) DO UPDATE
#                     SET tweet_json = EXCLUDED.tweet_json;
#                     ''',
#                     tweet_url, json.dumps(tweet_json)
#                 )
#                 print(f"✅ Inserted/Updated tweet: {tweet_url}")
#             except Exception as e:
#                 print(f"❌ Error inserting tweet {tweet_url}: {type(e).__name__} - {e}")
#                 raise
#
#     async def close(self):
#         await self.pool.close()
#         print("✅ Database pool closed.")




#----------------------------------------------------------------------------------------------



import asyncpg
from config import POSTGRES_CONFIG
import json

class AsyncDBManager:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def create(cls):
        pool = await asyncpg.create_pool(**POSTGRES_CONFIG)
        return cls(pool)

    async def create_output_table(self, table_name: str):

        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "Twitter"."{table_name}" (
                    id SERIAL PRIMARY KEY,
                    tweet_url TEXT UNIQUE,
                    tweet_id TEXT,
                    name TEXT,
                    screen_name TEXT,
                    created_at TEXT,
                    text TEXT,
                    retweet_count INT,
                    reply_count INT,
                    like_count INT,
                    quote_count INT,
                    repost_count INT,
                    total_views BIGINT,
                    bookmark_count INT,
                    raw_json JSONB,
                    inserted_at TIMESTAMP DEFAULT NOW()
                );
            """)
            print(f"✅ Output table '{table_name}' ensured with structured columns.")

    async def fetch_pending_tweets(self, input_table: str):
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                f'SELECT * FROM "Twitter"."{input_table}" WHERE status = $1',
                'pending'
            )

    async def update_status(self, input_table: str, tweet_url: str, status: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "Twitter"."{input_table}" SET status = $1 WHERE "tweetUrl" = $2',
                status, tweet_url
            )

    async def insert_parsed_tweet(self, table_name: str, tweet_url: str, parsed: dict, raw_json: dict):

        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    f"""
                    INSERT INTO "Twitter"."{table_name}" (
                        tweet_url, tweet_id, name, screen_name, created_at, text,
                        retweet_count, reply_count, like_count, quote_count, repost_count,
                        total_views, bookmark_count, raw_json
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6,
                        $7, $8, $9, $10,
                        $11, $12, $13 ,$14
                    )
                    ON CONFLICT (tweet_url) DO UPDATE SET
                        tweet_id = EXCLUDED.tweet_id,
                        name = EXCLUDED.name,
                        screen_name = EXCLUDED.screen_name,
                        created_at = EXCLUDED.created_at,
                        text = EXCLUDED.text,
                        retweet_count = EXCLUDED.retweet_count,
                        reply_count = EXCLUDED.reply_count,
                        like_count = EXCLUDED.like_count,
                        quote_count = EXCLUDED.quote_count,
                        repost_count = EXCLUDED.repost_count,
                        total_views = EXCLUDED.total_views,
                        bookmark_count = EXCLUDED.bookmark_count,
                        raw_json = EXCLUDED.raw_json;
                    """,
                    tweet_url,
                    parsed.get("tweet_id"),
                    parsed.get("name"),
                    parsed.get("screen_name"),
                    parsed.get("created_at"),
                    parsed.get("text"),
                    int(parsed.get("retweet_count", 0) or 0),
                    int(parsed.get("reply_count", 0) or 0),
                    int(parsed.get("like_count", 0) or 0),
                    int(parsed.get("quote_count", 0) or 0),
                    int(parsed.get("repost_count",0) or 0),
                    int(parsed.get("total_views", 0) or 0),
                    int(parsed.get("bookmark_count", 0) or 0),
                    json.dumps(raw_json) if raw_json else None,
                )
                print(f"✅ Inserted structured tweet: {tweet_url}")
            except Exception as e:
                print(f"❌ Error inserting tweet {tweet_url}: {type(e).__name__} - {e}")
                raise

    async def close(self):
        await self.pool.close()
        print("✅ Database pool closed.")
