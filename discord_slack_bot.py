import os
import json
import re
import asyncio
import logging
from collections import deque
from typing import Dict, Optional, Set, Any, List

import aiohttp
import discord
from discord import Intents
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK", "").strip()
SLACK_WEBHOOK_MAP_RAW = os.getenv("SLACK_WEBHOOK_MAP", "").strip()
CHANNEL_IDS: Set[int] = {
    int(cid.strip()) for cid in os.getenv("CHANNEL_IDS", "").split(",") if cid.strip()
}
MAX_MESSAGE_CHARS = int(os.getenv("MAX_MESSAGE_CHARS", "0"))

if not DISCORD_TOKEN:
    raise SystemExit("Missing DISCORD_TOKEN in environment.")
if not CHANNEL_IDS:
    raise SystemExit("Set CHANNEL_IDS to the IDs of channels in YOUR server that receive followed announcements.")
if not SLACK_WEBHOOK and not SLACK_WEBHOOK_MAP_RAW:
    raise SystemExit("Provide SLACK_WEBHOOK or SLACK_WEBHOOK_MAP.")

SLACK_WEBHOOK_MAP: Dict[str, str] = {}
if SLACK_WEBHOOK_MAP_RAW:
    try:
        SLACK_WEBHOOK_MAP = json.loads(SLACK_WEBHOOK_MAP_RAW)
        if not isinstance(SLACK_WEBHOOK_MAP, dict):
            raise ValueError("SLACK_WEBHOOK_MAP must be a JSON object")
    except Exception as e:
        raise SystemExit(f"Invalid SLACK_WEBHOOK_MAP JSON: {e}")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("relay")

# --------- Helpers ---------

def to_slack_md(text: str) -> str:
    """Lightweight conversion from Discord markdown to Slack mrkdwn."""
    if not text:
        return ""
    # bold+italics, bold, underline→bold, italics, strikethrough
    text = re.sub(r"\*\*\*(.+?)\*\*\*", r"_*\1*_", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"*\1*", text)
    text = re.sub(r"__(.+?)__", r"*\1*", text)
    text = re.sub(r"(?<!\*)\*(.+?)\*(?!\*)", r"_\1_", text)
    text = re.sub(r"~~(.+?)~~", r"~\1~", text)
    return text

def pick_webhook(channel_id: int) -> str:
    """Return the Slack webhook for a given Discord channel."""
    if SLACK_WEBHOOK_MAP:
        wh = SLACK_WEBHOOK_MAP.get(str(channel_id))
        if not wh:
            raise RuntimeError(f"No Slack webhook mapped for channel {channel_id}")
        return wh
    return SLACK_WEBHOOK

def clamp(s: str, limit: int) -> str:
    if limit and len(s) > limit:
        return s[: max(0, limit - 3)] + "..."
    return s

# --------- Discord Client ---------

intents = Intents.none()
intents.guilds = True
intents.messages = True
intents.message_content = True   # enable in Dev Portal for your bot

class RelayClient(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        # simple dedupe to prevent re-posting same message id for 15 minutes
        self.seen_ids: deque[str] = deque(maxlen=2000)

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        self.worker_task = asyncio.create_task(self._worker())
        log.info("Worker started")

    async def close(self) -> None:
        if self.http_session:
            await self.http_session.close()
        await super().close()

    async def _worker(self) -> None:
        """Deliver payloads to Slack with 429 handling."""
        assert self.http_session is not None
        while True:
            payload = await self.queue.get()
            try:
                webhook = payload.pop("_webhook")
                msg_id = payload.pop("_msg_id", None)
                author = payload.pop("_author", None)
                await self._post_to_slack(webhook, payload)
                log.info(f"Sent Message to Slack (msg_id={msg_id}, author={author})")
            except Exception as e:
                log.error(f"Slack post failed: {e}")
            finally:
                self.queue.task_done()

    async def _post_to_slack(self, webhook: str, payload: Dict[str, Any]) -> None:
        assert self.http_session is not None
        backoff = 1.0
        for attempt in range(6):
            async with self.http_session.post(webhook, json=payload) as resp:
                if resp.status == 429:
                    ra = resp.headers.get("Retry-After")
                    wait = float(ra) if ra and ra.isdigit() else backoff
                    log.warning(f"Slack rate limited (429). Retrying in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    backoff = min(backoff * 2, 30)
                    continue
                if 200 <= resp.status < 300:
                    return
                body = await resp.text()
                raise RuntimeError(f"Slack HTTP {resp.status}: {body}")

    # ---- Discord events ----

    async def on_ready(self):
        log.info(f"Logged in as {self.user} (ID: {self.user.id})")
        log.info(f"Watching channels: {sorted(CHANNEL_IDS)}")

    async def on_message(self, message: discord.Message):
        # Only forward from configured channels in your server
        if message.guild is None or message.channel.id not in CHANNEL_IDS:
            return

        # Only forward announcement/crosspost messages (not regular user messages)
        # Check if message is a crosspost or has the crossposted flag
        is_announcement = (
            message.flags.crossposted or
            message.type in (discord.MessageType.default, discord.MessageType.reply) and message.reference
        )
        if not is_announcement:
            log.info(f"Skip Message: Not an Announcement (msg_id={message.id}, author={message.author.display_name})")
            return

        # Deduplicate
        msg_key = f"{message.id}:{message.edited_at}"
        if msg_key in self.seen_ids:
            return
        self.seen_ids.append(msg_key)

        # Get original message info if this is a crosspost
        original_guild_name = None
        original_channel_name = None
        original_jump_url = message.jump_url

        if message.reference and message.reference.resolved:
            # This is a crosspost - get the original message details
            original_msg = message.reference.resolved
            if isinstance(original_msg, discord.Message):
                original_guild_name = original_msg.guild.name if original_msg.guild else None
                original_channel_name = original_msg.channel.name if original_msg.channel else None
                original_jump_url = original_msg.jump_url

        # Prepare content
        content = to_slack_md(message.content or "")
        if MAX_MESSAGE_CHARS:
            content = clamp(content, MAX_MESSAGE_CHARS)

        author = message.author.display_name

        # Build header with link to original Discord message
        header_text = f"<{original_jump_url}|:loudspeaker: Announcement"
        if original_guild_name:
            header_text += f" from {original_guild_name}"
            if original_channel_name:
                header_text += f" #{original_channel_name}"
        header_text += f">"
        if author:
            header_text += f" by `{author}`"

        header = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": header_text},
        }

        # Process embeds
        embed_lines: List[str] = []
        for e in message.embeds:
            # Only surface human-useful fields (title/desc/url)
            t = e.title or ""
            d = e.description or ""
            u = e.url or ""
            block = "\n".join(filter(None, [f"*{clamp(t, 256)}*" if t else "", to_slack_md(clamp(d, 3000)), u]))
            if block.strip():
                embed_lines.append(block)

        attach_lines = [a.url for a in message.attachments] if message.attachments else []

        # Build message body
        body_parts: List[str] = []
        if content:
            body_parts.append(content)
        if embed_lines:
            body_parts.append("\n*Embeds:*")
            body_parts.extend(embed_lines)
        if attach_lines:
            body_parts.append("\n*Attachments:*")
            body_parts.extend(attach_lines)

        blocks = [header]
        if body_parts:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(body_parts)}})

        payload: Dict[str, Any] = {
            "blocks": blocks,
            "unfurl_links": False,
            "unfurl_media": False,
            "_webhook": pick_webhook(message.channel.id),
            "_msg_id": message.id,
            "_author": message.author.display_name,
        }

        await self.queue.put(payload)

    # Optional: forward edits (comment out if you don’t want this)
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        await self.on_message(after)

def main():
    client = RelayClient()
    client.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()
