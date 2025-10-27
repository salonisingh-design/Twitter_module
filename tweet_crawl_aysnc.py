import asyncio
import aiohttp
import random
from cookie_dynamic_handling_async import TokenManager
from proxy_detail import proxy_info


async def extract_tweet_id(tweet_url: str) -> str:
    try:
        return tweet_url.strip("/").split("/")[-1]
    except Exception:
        return None


async def fetch_tweet_data(session: aiohttp.ClientSession, token_mgr: TokenManager, tweet_url: str, retries: int = 3):
    tweet_id = await extract_tweet_id(tweet_url)
    if not tweet_id:
        print(f"⚠️ Invalid tweet URL: {tweet_url}")
        return None, "invalid_url"

    api_url = ( f"https://api.x.com/graphql/WvlrBJ2bz8AuwoszWyie8A/TweetResultByRestId?" f"variables=%7B%22tweetId%22%3A%22{tweet_id}%22%2C%22includePromotedContent%22%3Atrue%2C%22withBirdwatchNotes%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withCommunity%22%3Atrue%7D" f"&features=%7B%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22premium_content_api_read_enabled%22%3Afalse%2C%22communities_web_enable_tweet_community_results_fetch%22%3Atrue%2C%22c9s_tweet_anatomy_moderator_badge_enabled%22%3Atrue%2C%22responsive_web_grok_analyze_button_fetch_trends_enabled%22%3Afalse%2C%22responsive_web_grok_analyze_post_followups_enabled%22%3Afalse%2C%22responsive_web_jetfuel_frame%22%3Atrue%2C%22responsive_web_grok_share_attachment_enabled%22%3Atrue%2C%22articles_preview_enabled%22%3Atrue%2C%22responsive_web_edit_tweet_api_enabled%22%3Atrue%2C%22graphql_is_translatable_rweb_tweet_is_translatable_enabled%22%3Atrue%2C%22view_counts_everywhere_api_enabled%22%3Atrue%2C%22longform_notetweets_consumption_enabled%22%3Atrue%2C%22responsive_web_twitter_article_tweet_consumption_enabled%22%3Atrue%2C%22tweet_awards_web_tipping_enabled%22%3Afalse%2C%22responsive_web_grok_show_grok_translated_post%22%3Afalse%2C%22responsive_web_grok_analysis_button_from_backend%22%3Atrue%2C%22creator_subscriptions_quote_tweet_preview_enabled%22%3Afalse%2C%22freedom_of_speech_not_reach_fetch_enabled%22%3Atrue%2C%22standardized_nudges_misinfo%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled%22%3Atrue%2C%22longform_notetweets_rich_text_read_enabled%22%3Atrue%2C%22longform_notetweets_inline_media_enabled%22%3Atrue%2C%22payments_enabled%22%3Afalse%2C%22profile_label_improvements_pcf_label_in_post_enabled%22%3Atrue%2C%22responsive_web_profile_redirect_enabled%22%3Afalse%2C%22rweb_tipjar_consumption_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Atrue%2C%22responsive_web_grok_image_annotation_enabled%22%3Atrue%2C%22responsive_web_grok_imagine_annotation_enabled%22%3Atrue%2C%22responsive_web_grok_community_note_auto_translation_is_enabled%22%3Afalse%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D" f"&fieldToggles=%7B%22withArticleRichContentState%22%3Atrue%2C%22withArticlePlainText%22%3Afalse%7D" )

    for attempt in range(1, retries + 1):
        try:
            # Refresh headers or tokens
            try:
                headers = await token_mgr.get_headers()
            except RuntimeError:
                await token_mgr.refresh_tokens(tweet_url)
                headers = await token_mgr.get_headers()

            await asyncio.sleep(random.uniform(0.5, 1.5))  # small jitter
            proxy = proxy_info()
            async with session.get(api_url, headers=headers, timeout=25, proxy=proxy) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data, "done"
                elif resp.status in (401, 403):
                    # token issue
                    print(f"🔁 Token issue for {tweet_url}, refreshing...")
                    await token_mgr.refresh_tokens(tweet_url)
                    await asyncio.sleep(2 ** attempt)
                elif resp.status == 429:
                    # rate-limit
                    wait_time = 15 * attempt
                    print(f"⏳ Rate limited for {tweet_url}, sleeping {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    print(f"⚠️ Non-200 ({resp.status}) for {tweet_url}, retry {attempt}/{retries}")
                    await asyncio.sleep(2 ** attempt)
        except (aiohttp.ClientConnectionError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"🌐 Network error on {tweet_url}: {e}")
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            print(f"❌ Unexpected error for {tweet_url}: {type(e).__name__} - {e}")
            await asyncio.sleep(2 ** attempt)

    print(f"❌ Failed to fetch after {retries} retries: {tweet_url}")
    return None, "error"
