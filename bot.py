from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import CommandStart
from database import search_logs
import html

router = Router()

@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "<b>🤖 Log Search Bot Ready</b>\n\n"
        "Send me any keyword, IP address, or email to search the database.\n"
        "<i>Search is optimized for speed.</i>",
        parse_mode="HTML"
    )

@router.message(F.text)
async def handle_search(message: Message):
    query = message.text.strip()
    
    if len(query) < 2:
        await message.answer("⚠️ Query too short.")
        return

    status_msg = await message.answer("🔍 Searching...")
    
    try:
        results, total_count = await search_logs(query, limit=20)
        
        if not results:
            await status_msg.edit_text(f"❌ No results found for: <code>{html.escape(query)}</code>", parse_mode="HTML")
            return

        # Format Output
        response_lines = [f"<b>Results for:</b> <code>{html.escape(query)}</code>\n"]
        
        for i, line in enumerate(results, 1):
            # Truncate very long lines to keep UI clean
            clean_line = html.escape(line[:300]) 
            response_lines.append(f"<code>{i}. {clean_line}</code>")

        if total_count > 20:
            response_lines.append(f"\n<i>...and {total_count - 20} more results hidden.</i>")
        
        response_text = "\n".join(response_lines)
        
        # Telegram limit is 4096 chars
        if len(response_text) > 4000:
            response_text = response_text[:4000] + "\n...(truncated)"

        await status_msg.edit_text(response_text, parse_mode="HTML")

    except Exception as e:
        await status_msg.edit_text(f"⚠️ Database Error: {str(e)}")
