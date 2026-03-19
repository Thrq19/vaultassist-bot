import asyncio
import logging
import os
import time
import html
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.types import Message, BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeAllGroupChats, BotCommandScopeChat, CallbackQuery, InlineKeyboardButton, ChatMemberUpdated
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from supabase import create_client, Client
from aiohttp import web 

# ==========================================
# 1. SETUP & KONFIGURASI
# ==========================================
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUDO_PASSWORD = os.getenv("SUDO_PASSWORD", "rahasia123")
CONTACT_USERNAME = os.getenv("CONTACT_USERNAME", "@admin") 

BLACKLIST_EXT = ['.exe', '.bat', '.cmd', '.msi', '.apk', '.sh', '.vbs', '.scr']

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
dp = Dispatcher()

album_cache = {}
last_upload_time = {}
user_search_cache = {} 
wizard_cache = {} # Cache untuk mengingat preferensi caption user

class LoginState(StatesGroup):
    waiting_for_password = State()

class WizardState(StatesGroup):
    waiting_for_rename = State()
    waiting_for_caption = State()

async def db_exec(query_func):
    return await asyncio.to_thread(query_func)

async def get_allowed_groups(bot: Bot, user_id: int):
    user_res = await db_exec(lambda: supabase.table("users").select("role").eq("user_id", user_id).execute())
    is_superadmin = user_res.data and user_res.data[0].get('role') == 'superadmin'

    grp_res = await db_exec(lambda: supabase.table("groups").select("*").execute())
    all_groups = grp_res.data

    allowed = []
    for g in all_groups:
        if is_superadmin:
            allowed.append(g)
            continue
        
        try:
            member = await bot.get_chat_member(chat_id=g['group_id'], user_id=user_id)
            if member.status in ['creator', 'administrator']:
                allowed.append(g) 
            elif member.status in ['member', 'restricted'] and g.get('allow_members', False):
                allowed.append(g) 
        except Exception:
            pass 
    return allowed

# --- FUNGSI BANTUAN BAHASA & INSTRUKSI ---
async def get_user_lang(user_id):
    res = await db_exec(lambda: supabase.table("bot_settings").select("setting_value").eq("setting_key", f"lang_{user_id}").execute())
    return res.data[0]['setting_value'] if res.data else "id"

async def send_help_instructions(bot: Bot, chat_id: int, user_id: int):
    lang = await get_user_lang(user_id)
    user_res = await db_exec(lambda: supabase.table("users").select("role").eq("user_id", user_id).execute())
    is_superadmin = user_res.data and user_res.data[0].get('role') == 'superadmin'

    if lang == "id":
        teks = "📚 **PANDUAN PENGGUNAAN GROUP VAULT ASSISTANT**\n\n"
        teks += "👤 **1. PANDUAN PENGGUNA (USER BIASA)**\n"
        teks += "*Cara Mengarsipkan File (Upload):*\n"
        teks += "1️⃣ Kirim file, foto, atau dokumen secara langsung (Japri) ke chat bot ini.\n"
        teks += "2️⃣ Ketik `/queue` untuk membuka keranjang antrean file Anda.\n"
        teks += "3️⃣ Klik tombol **➡️ Proses File Ini**, lalu ikuti panduan pengaturan (Ganti Nama, Tampilkan Caption, Tambah Caption Tambahan).\n"
        teks += "4️⃣ Pilih *Grup* dan *Topik (Folder)* tujuannya. File otomatis terkirim & tersimpan!\n\n"
        teks += "*Cara Mencari File (Download):*\n"
        teks += "• Ketik `/files` untuk menelusuri arsip layaknya membuka folder di komputer.\n"
        teks += "• Ketik `/search <kata_kunci>` untuk mencari nama file dengan cepat.\n\n"
        
        teks += "👮 **2. PANDUAN ADMIN GRUP**\n"
        teks += "• **Setup Awal:** Masukkan bot ini ke grup Telegram Anda dan pastikan menjadikannya Admin.\n"
        teks += "• **Auto-Folder:** Setiap Anda membuat *Topik* baru di grup Telegram, bot akan otomatis mencatatnya sebagai Folder Arsip.\n"
        teks += "• **Atur Hak Akses:** Ketik `/group_settings on` di dalam grup agar *member biasa* diizinkan menyimpan file ke grup tersebut via bot.\n"
        teks += "• **CCTV Otomatis:** Setiap media yang dikirim langsung di dalam grup akan otomatis diarsipkan.\n\n"

        if is_superadmin:
            teks += "👑 **3. PANDUAN SUPER ADMIN (SUDO)**\n"
            teks += "• `/set_backup` : Ketik perintah ini di *Grup Rahasia* Anda untuk menjadikannya Brankas Utama. Semua file dari grup mana pun akan di-copy ke sini diam-diam.\n"
            teks += "• `/stats` : Buka Dashboard untuk melihat statistik Database.\n"
            teks += "• `/set_gc <angka>` : Atur sistem pembersih otomatis.\n"
            teks += "• Anda memiliki wewenang penuh pada arsip lintas grup.\n\n"
        
        teks += f"📞 *Butuh bantuan teknis? Hubungi:* {CONTACT_USERNAME}"
    else:
        teks = "📚 **GROUP VAULT ASSISTANT USER GUIDE**\n\n"
        teks += "👤 **1. REGULAR USER GUIDE**\n"
        teks += "*How to Archive a File (Upload):*\n"
        teks += "1️⃣ Send a file, photo, or document directly (DM) to this bot.\n"
        teks += "2️⃣ Type `/queue` to open your upload queue.\n"
        teks += "3️⃣ Click **➡️ Process This File**, then follow the setup wizard (Rename, Show Caption, Add Custom Caption).\n"
        teks += "4️⃣ Select the destination *Group* and *Topic (Folder)*. The file will be sent & saved!\n\n"
        teks += "*How to Find a File (Download):*\n"
        teks += "• Type `/files` to browse archives.\n"
        teks += "• Type `/search <keyword>` to quickly find a file.\n\n"
        
        teks += "👮 **2. GROUP ADMIN GUIDE**\n"
        teks += "• **Initial Setup:** Add this bot to your Telegram group and promote it to Admin.\n"
        teks += "• **Auto-Folder:** New Topics in the group are automatically registered as Archive Folders.\n"
        teks += "• **Set Permissions:** Type `/group_settings on` inside the group to allow members to upload.\n"
        teks += "• **Auto-CCTV:** Media sent directly in the group is automatically archived.\n\n"

        if is_superadmin:
            teks += "👑 **3. SUPER ADMIN GUIDE (SUDO)**\n"
            teks += "• `/set_backup` : Set a Secret Group as the Main Vault. All files are silently copied here.\n"
            teks += "• `/stats` : Open the Database Dashboard.\n"
            teks += "• `/set_gc <number>` : Set the auto-clean system.\n"
            teks += "• Absolute authority across all groups.\n\n"
        
        teks += f"📞 *Need technical support? Contact:* {CONTACT_USERNAME}"

    await bot.send_message(chat_id=chat_id, text=teks, parse_mode="Markdown")

# ==========================================
# FUNGSI BANTUAN UI
# ==========================================
async def get_queue_ui(user_id, page=0):
    PER_PAGE = 10
    start_idx = page * PER_PAGE
    end_idx = start_idx + PER_PAGE - 1
    
    response = await db_exec(lambda: supabase.table("upload_queue").select("*", count="exact").eq("user_id", user_id).range(start_idx, end_idx).execute())
    data = response.data
    total_items = response.count or 0
    
    if not data and total_items == 0:
        return "🛒 <b>Keranjang Antrean Kosong!</b>\n\nSilakan kirim file baru.", None
        
    total_pages = (total_items + PER_PAGE - 1) // PER_PAGE
    
    teks = f"🛒 <b>Daftar Antrean File (Hal {page+1}/{total_pages}):</b>\n\n"
    builder = InlineKeyboardBuilder()
    
    num_buttons = []
    for i, item in enumerate(data):
        real_idx = start_idx + i + 1
        nama = html.escape(item['original_name'])
        status = "Menunggu Diproses" if item['status'] in ['naming', 'active_naming'] else "Menunggu Grup/Topik"
        fid = item['file_unique_id']
        
        teks += f"<b>{real_idx}.</b> <code>{nama}</code>\n   └ <i>{status}</i>\n"
        num_buttons.append(InlineKeyboardButton(text=f"{real_idx}", callback_data=f"qnum_{fid}"))
    
    builder.row(*num_buttons, width=5)
    
    nav_buttons = []
    if page > 0: nav_buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"qpage_{page-1}"))
    if page < total_pages - 1: nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"qpage_{page+1}"))
    if nav_buttons: builder.row(*nav_buttons)
        
    if total_items > 1:
        builder.row(InlineKeyboardButton(text=f"📦 Proses Semua Sekaligus ({total_items} File)", callback_data="bulk_start"))
    builder.row(InlineKeyboardButton(text="🧹 Bersihkan Semua Antrean", callback_data="clear_all_queue"))
        
    teks += "\n<i>*Pilih angka untuk mengatur file milikmu.</i>"
    return teks, builder.as_markup()

async def get_search_ui(bot: Bot, user_id: int, page=0):
    query = user_search_cache.get(user_id, "")
    if not query: return "Pencarian kadaluarsa. Silakan ketik ulang <code>/search kata_kunci</code>.", None

    allowed_groups = await get_allowed_groups(bot, user_id)
    if not allowed_groups: return "⛔ Anda tidak memiliki akses ke arsip grup manapun.", None
    allowed_group_ids = [str(g['group_id']) for g in allowed_groups]

    PER_PAGE = 10
    start_idx = page * PER_PAGE
    end_idx = start_idx + PER_PAGE - 1

    response = await db_exec(lambda: supabase.table("files").select("*", count="exact").ilike("display_name", f"%{query}%").in_("group_id", allowed_group_ids).range(start_idx, end_idx).execute())
    data = response.data
    total_items = response.count or 0
    
    if not data: return f"🔍 Tidak ditemukan arsip dengan kata kunci: <b>{html.escape(query)}</b> (Atau Anda tidak memiliki akses).", None
        
    total_pages = (total_items + PER_PAGE - 1) // PER_PAGE

    group_ids_in_page = list(set([item['group_id'] for item in data]))
    g_res = await db_exec(lambda: supabase.table("groups").select("group_id, group_name").in_("group_id", group_ids_in_page).execute())
    g_map = {g['group_id']: g['group_name'] for g in (g_res.data or [])}

    t_res = await db_exec(lambda: supabase.table("topics").select("group_id, message_thread_id, topic_name").in_("group_id", group_ids_in_page).execute())
    t_map = {(t['group_id'], t['message_thread_id']): t['topic_name'] for t in (t_res.data or [])}
    
    teks = f"🔍 <b>Hasil Pencarian: '{html.escape(query)}' (Hal {page+1}/{total_pages}):</b>\nTotal: {total_items} file ditemukan.\n\n"
    builder = InlineKeyboardBuilder()
    
    num_buttons = []
    for i, item in enumerate(data):
        real_idx = start_idx + i + 1
        nama = html.escape(item['display_name'])
        tipe = item['media_type']
        fid = item['file_unique_id']
        grp_id = item['group_id']
        thrd_id = item.get('message_thread_id')

        g_name = g_map.get(grp_id, "Grup Tidak Diketahui")
        t_name = t_map.get((grp_id, thrd_id), "General / Tidak Terdaftar") if thrd_id else "General / Tidak Terdaftar"
        
        teks += f"<b>{real_idx}.</b> <code>{nama}</code> <i>({tipe})</i>\n   └ 📍 {g_name} (📂 {t_name})\n"
        num_buttons.append(InlineKeyboardButton(text=f"{real_idx}", callback_data=f"snum_{fid}"))
    
    builder.row(*num_buttons, width=5)
    
    nav_buttons = []
    if page > 0: nav_buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"spage_{page-1}"))
    if page < total_pages - 1: nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"spage_{page+1}"))
    if nav_buttons: builder.row(*nav_buttons)
        
    teks += "\n<i>*Klik angka untuk melihat, memindah, atau menyalin file.</i>"
    return teks, builder.as_markup()

async def get_list_groups_ui(bot: Bot, user_id: int):
    allowed_groups = await get_allowed_groups(bot, user_id)
    if not allowed_groups: 
        return "📭 <b>Akses Ditolak!</b>\nAnda tidak memiliki akses ke arsip grup manapun.", None
        
    teks = "🗂️ <b>Daftar Grup:</b>\nPilih grup untuk menelusuri arsip.\n\n"
    builder = InlineKeyboardBuilder()
    for grup in allowed_groups:
        builder.button(text=f"🏢 {grup['group_name']}", callback_data=f"lgrp_{grup['group_id']}")
    builder.adjust(1)
    return teks, builder.as_markup()

async def get_list_topics_ui(group_id):
    grup_res = await db_exec(lambda: supabase.table("groups").select("group_name").eq("group_id", group_id).execute())
    nama_grup = grup_res.data[0]['group_name'] if grup_res.data else "Grup"
    
    res = await db_exec(lambda: supabase.table("topics").select("*").eq("group_id", group_id).execute())
    data = res.data
    
    builder = InlineKeyboardBuilder()
    if not data:
        builder.button(text="🔙 Kembali", callback_data="list_groups")
        return f"🏢 <b>{nama_grup}</b>\n\nBelum ada folder di grup ini. Silakan buat topik baru di Telegram.", builder.as_markup()
        
    teks = f"🏢 <b>{nama_grup}</b>\nPilih topik untuk melihat file:\n\n"
    for topik in data:
        builder.button(text=f"📂 {topik['topic_name']}", callback_data=f"ltop_{group_id}_{topik['message_thread_id']}")
    builder.button(text="🔙 Kembali ke Daftar Grup", callback_data="list_groups")
    builder.adjust(1)
    return teks, builder.as_markup()

async def get_list_files_ui(group_id, thread_id, page=0):
    topik_res = await db_exec(lambda: supabase.table("topics").select("topic_name").eq("group_id", group_id).eq("message_thread_id", thread_id).execute())
    nama_topik = topik_res.data[0]['topic_name'] if topik_res.data else "Topik"
    
    PER_PAGE = 10
    start_idx = page * PER_PAGE
    end_idx = start_idx + PER_PAGE - 1
    
    res = await db_exec(lambda: supabase.table("files").select("*", count="exact").eq("group_id", group_id).eq("message_thread_id", thread_id).range(start_idx, end_idx).execute())
    data = res.data
    total_items = res.count or 0
    
    builder = InlineKeyboardBuilder()
    if not data:
        builder.button(text="🔙 Kembali", callback_data=f"lgrp_{group_id}")
        return f"📂 <b>{nama_topik}</b>\n\nKosong. Belum ada file di topik ini.", builder.as_markup()
        
    total_pages = (total_items + PER_PAGE - 1) // PER_PAGE
    
    teks = f"📂 <b>{nama_topik}</b> (Hal {page+1}/{total_pages}):\nTotal: {total_items} file.\n\n"
    num_buttons = []
    for i, item in enumerate(data):
        real_idx = start_idx + i + 1
        nama = html.escape(item['display_name'])
        tipe = item['media_type']
        fid = item['file_unique_id']
        teks += f"<b>{real_idx}.</b> <code>{nama}</code> <i>({tipe})</i>\n"
        num_buttons.append(InlineKeyboardButton(text=f"{real_idx}", callback_data=f"fnum_{fid}"))
    
    builder.row(*num_buttons, width=5)
    
    nav_buttons = []
    if page > 0: nav_buttons.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"lpage_{group_id}_{thread_id}_{page-1}"))
    if page < total_pages - 1: nav_buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"lpage_{group_id}_{thread_id}_{page+1}"))
    if nav_buttons: builder.row(*nav_buttons)
        
    builder.row(InlineKeyboardButton(text="🔙 Kembali ke Daftar Topik", callback_data=f"lgrp_{group_id}"))
    teks += "\n<i>*Klik angka untuk memindah/menyalin file.</i>"
    return teks, builder.as_markup()

# ==========================================
# 2. HANDLER PERINTAH DASAR & SENSOR OTOMATIS
# ==========================================

@dp.message(CommandStart(), F.chat.type == "private")
async def handle_start_private(message: Message):
    nama_user = message.from_user.full_name
    user_id = message.from_user.id
    try:
        cek = await db_exec(lambda: supabase.table("users").select("role").eq("user_id", user_id).execute())
        if not cek.data:
            await db_exec(lambda: supabase.table("users").insert({"user_id": user_id, "full_name": nama_user, "role": "user"}).execute())
    except Exception: pass
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🇮🇩 Bahasa Indonesia", callback_data="setlang_id")
    builder.button(text="🇬🇧 English", callback_data="setlang_en")
    builder.adjust(2)
    
    await message.answer(f"Halo {nama_user}! 🚀\nAku VaultAssist, asisten Group Vault kamu.\n\n🌍 <b>Pilih bahasa / Choose your language:</b>", reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("setlang_"))
async def save_language(callback: CallbackQuery):
    lang_choice = callback.data.split("_")[1]
    user_id = callback.from_user.id
    try:
        await db_exec(lambda: supabase.table("bot_settings").upsert({"setting_key": f"lang_{user_id}", "setting_value": lang_choice}).execute())
        await callback.message.delete()
        if lang_choice == "id":
            await callback.message.answer("✅ Bahasa Indonesia berhasil diatur!\nKirimkan file apapun ke chat ini untuk mulai mengarsipkan.")
        else:
            await callback.message.answer("✅ English selected successfully!\nSend any file to this chat to start archiving.")
        
        await send_help_instructions(callback.bot, callback.message.chat.id, user_id)
    except Exception as e:
        await callback.answer("Gagal mengatur bahasa.", show_alert=True)

@dp.message(Command("help"))
async def help_cmd(message: Message):
    if message.chat.type != "private": return
    await send_help_instructions(message.bot, message.chat.id, message.from_user.id)

@dp.message(F.new_chat_title)
async def auto_update_group_name(message: Message):
    new_name = message.new_chat_title
    group_id = message.chat.id
    try: await db_exec(lambda: supabase.table("groups").update({"group_name": new_name}).eq("group_id", group_id).execute())
    except Exception: pass

@dp.message(F.migrate_to_chat_id)
async def auto_handle_group_migration(message: Message):
    old_id = message.chat.id
    new_id = message.migrate_to_chat_id
    try:
        await db_exec(lambda: supabase.table("groups").delete().eq("group_id", old_id).execute())
        await db_exec(lambda: supabase.table("groups").upsert({"group_id": new_id, "group_name": message.chat.title}).execute())
    except Exception: pass

@dp.my_chat_member()
async def auto_handle_group_status(event: ChatMemberUpdated):
    if event.chat.type in ['group', 'supergroup']:
        status = event.new_chat_member.status
        if status in ['member', 'administrator']:
            try: await db_exec(lambda: supabase.table("groups").upsert({"group_id": event.chat.id, "group_name": event.chat.title}).execute())
            except Exception: pass
        elif status in ['kicked', 'left']:
            try:
                await db_exec(lambda: supabase.table("groups").delete().eq("group_id", event.chat.id).execute())
                await db_exec(lambda: supabase.table("topics").delete().eq("group_id", event.chat.id).execute())
            except Exception: pass

@dp.message(F.forum_topic_created)
async def auto_register_new_topic(message: Message):
    group_id = message.chat.id
    group_name = message.chat.title
    thread_id = message.message_thread_id
    topic_name = message.forum_topic_created.name
    try:
        await db_exec(lambda: supabase.table("groups").upsert({"group_id": group_id, "group_name": group_name}).execute())
        cek_topik = await db_exec(lambda: supabase.table("topics").select("*").eq("group_id", group_id).eq("message_thread_id", thread_id).execute())
        if not cek_topik.data:
            await db_exec(lambda: supabase.table("topics").insert({"group_id": group_id, "message_thread_id": thread_id, "topic_name": topic_name}).execute())
            await message.reply(f"✅ <b>Topik Terdeteksi!</b>\nFolder <b>'{topic_name}'</b> otomatis masuk ke database arsip.", parse_mode="HTML")
    except Exception as e: print(f"Gagal auto-register topik: {e}")

@dp.message(F.forum_topic_edited)
async def auto_rename_topic(message: Message):
    if not message.forum_topic_edited.name: return 
    new_name = message.forum_topic_edited.name
    try: await db_exec(lambda: supabase.table("topics").update({"topic_name": new_name}).eq("group_id", message.chat.id).eq("message_thread_id", message.message_thread_id).execute())
    except Exception: pass

@dp.message(CommandStart(), F.chat.type.in_(["group", "supergroup"]))
async def handle_start_group(message: Message):
    try:
        await db_exec(lambda: supabase.table("groups").upsert({"group_id": message.chat.id, "group_name": message.chat.title}).execute())
        await message.reply("✅ <b>Grup Tersinkronisasi!</b>\nGrup ini telah masuk ke dalam radar arsip VaultAssist.", parse_mode="HTML")
    except Exception: await message.reply("❌ Gagal sinkronisasi grup.")

@dp.message(Command("sudo"))
async def sudo_cmd(message: Message, state: FSMContext):
    if message.chat.type != "private": return
    await message.reply("🔒 <b>SISTEM KEAMANAN VAULT ASSISTANT</b>\nMasukkan Password Super Admin:", parse_mode="HTML")
    await state.set_state(LoginState.waiting_for_password)

@dp.message(LoginState.waiting_for_password)
async def sudo_auth(message: Message, state: FSMContext):
    if message.text == SUDO_PASSWORD:
        await db_exec(lambda: supabase.table("users").update({"role": "superadmin"}).eq("user_id", message.from_user.id).execute())
        super_cmds = [
            BotCommand(command="start", description="Refresh Bot"),
            BotCommand(command="help", description="Panduan & Bantuan"),
            BotCommand(command="queue", description="Lihat antrean file"),
            BotCommand(command="files", description="[👑] Akses Seluruh Arsip"),
            BotCommand(command="search", description="[👑] Cari di Seluruh Grup"),
            BotCommand(command="list_topics", description="[👑] Lihat Semua Struktur"),
            BotCommand(command="stats", description="[👑] Dashboard Data"),
            BotCommand(command="set_gc", description="[👑] Set waktu auto-clean antrean"),
            BotCommand(command="set_backup", description="[👑] Set Brankas (Ketik di Grup)") 
        ]
        await message.bot.set_my_commands(super_cmds, scope=BotCommandScopeChat(chat_id=message.from_user.id))
        
        lang = await get_user_lang(message.from_user.id)
        if lang == "id": await message.reply("✅ <b>AKSES DIBERIKAN.</b>\n\nSelamat datang, Super Admin!\nKetik /help untuk membaca instruksi dewa Anda.", parse_mode="HTML")
        else: await message.reply("✅ <b>ACCESS GRANTED.</b>\n\nWelcome, Super Admin!\nType /help to read your god-mode instructions.", parse_mode="HTML")
    else:
        await message.reply("❌ Password Salah! Identitas ditolak.")
    await state.clear()

@dp.message(Command("set_gc"))
async def set_gc_duration(message: Message, command: CommandObject):
    if message.chat.type != "private": return
    user_id = message.from_user.id
    user_res = await db_exec(lambda: supabase.table("users").select("role").eq("user_id", user_id).execute())
    if not user_res.data or user_res.data[0].get('role') != 'superadmin':
        return await message.reply("⛔ Hanya Super Admin yang berhak mengubah pengaturan ini!")

    if not command.args or not command.args.isdigit():
        return await message.reply("⚠️ <b>Format Salah!</b>\nGunakan: <code>/set_gc [angka_dalam_jam]</code>\nContoh: <code>/set_gc 48</code> (untuk 2 hari).", parse_mode="HTML")

    hours = int(command.args.strip())
    if hours < 1: return await message.reply("⚠️ Angka minimal adalah 1 jam.")

    try:
        await db_exec(lambda: supabase.table("bot_settings").upsert({"setting_key": "gc_duration_hours", "setting_value": str(hours)}).execute())
        await message.reply(f"✅ <b>Pengaturan Diperbarui!</b>\n\nSistem Tukang Sapu (*Garbage Collector*) sekarang akan membersihkan semua file di antrean yang tidak diproses selama lebih dari <b>{hours} jam</b>.", parse_mode="HTML")
    except Exception as e:
        await message.reply(f"❌ Gagal menyimpan pengaturan: {e}")

@dp.message(Command("stats"))
async def bot_stats(message: Message):
    if message.chat.type != "private": return
    user_res = await db_exec(lambda: supabase.table("users").select("role").eq("user_id", message.from_user.id).execute())
    if not user_res.data or user_res.data[0].get('role') != 'superadmin':
        return await message.reply("⛔ Hanya Super Admin yang bisa melihat statistik database.")
    
    msg_wait = await message.reply("⏳ <i>Menghitung data real-time...</i>", parse_mode="HTML")
    
    try:
        f_res = await db_exec(lambda: supabase.table("files").select("*", count="exact").limit(1).execute())
        g_res = await db_exec(lambda: supabase.table("groups").select("*", count="exact").limit(1).execute())
        q_res = await db_exec(lambda: supabase.table("upload_queue").select("*", count="exact").limit(1).execute())
        t_res = await db_exec(lambda: supabase.table("topics").select("*", count="exact").limit(1).execute())
        
        gc_res = await db_exec(lambda: supabase.table("bot_settings").select("setting_value").eq("setting_key", "gc_duration_hours").execute())
        gc_hours = gc_res.data[0]['setting_value'] if gc_res.data else "48"

        teks = (
            "📊 <b>DASHBOARD STATISTIK VAULT ASSISTANT</b>\n\n"
            f"🏢 <b>Total Grup:</b> {g_res.count or 0}\n"
            f"📂 <b>Total Folder/Topik:</b> {t_res.count or 0}\n"
            f"📑 <b>Total File Terarsip:</b> {f_res.count or 0}\n"
            f"🛒 <b>File Nyangkut di Antrean:</b> {q_res.count or 0}\n\n"
            f"<i>*Auto-Clean Antrean: Aktif setiap {gc_hours} Jam.</i>"
        )
        await msg_wait.edit_text(teks, parse_mode="HTML")
    except Exception as e:
        await msg_wait.edit_text(f"❌ Gagal memuat statistik: {e}")

@dp.message(Command("queue"))
async def lihat_antrean(message: Message):
    if message.chat.type != "private": return
    try:
        teks, markup = await get_queue_ui(message.from_user.id, page=0)
        await message.reply(teks, reply_markup=markup, parse_mode="HTML") if markup else await message.reply(teks, parse_mode="HTML")
    except Exception as e: await message.reply(f"❌ Error: {e}")

@dp.message(Command("search"))
async def cari_file(message: Message, command: CommandObject):
    if message.chat.type != "private": return
    if not command.args: return await message.reply("⚠️ Gunakan: <code>/search kata kunci</code>", parse_mode="HTML")
    query = command.args.strip()
    user_id = message.from_user.id
    user_search_cache[user_id] = query
    try:
        teks, markup = await get_search_ui(message.bot, user_id, page=0)
        await message.reply(teks, reply_markup=markup, parse_mode="HTML") if markup else await message.reply(teks, parse_mode="HTML")
    except Exception as e: await message.reply(f"❌ Gagal mencari file: {e}")

@dp.message(Command("files"))
async def lihat_semua_file(message: Message):
    if message.chat.type != "private": return
    try:
        teks, markup = await get_list_groups_ui(message.bot, message.from_user.id)
        await message.reply(teks, reply_markup=markup, parse_mode="HTML") if markup else await message.reply(teks, parse_mode="HTML")
    except Exception as e: await message.reply(f"❌ Gagal memuat daftar file: {e}")

@dp.message(Command("list_topics"))
async def cek_daftar_topik(message: Message):
    if message.chat.type != "private": return
    try:
        response = await db_exec(lambda: supabase.table("groups").select("group_name, topics(topic_name)").execute())
        data_grup = response.data
        if not data_grup: return await message.reply("Belum ada grup yang terdaftar.", parse_mode="HTML")
        
        teks = "📚 <b>Daftar Grup & Topik Terdaftar:</b>\n\n"
        for grup in data_grup:
            teks += f"🏢 <b>{grup.get('group_name', 'Unnamed')}</b>\n"
            if grup.get('topics', []):
                for topik in grup['topics']: teks += f"   ├ 📂 {topik.get('topic_name')}\n"
            else: teks += "   └ <i>(Belum ada folder/topik)</i>\n"
            teks += "\n"
        await message.reply(teks, parse_mode="HTML")
    except Exception as e: await message.reply(f"❌ Error: {e}")

# ==========================================
# 3. HANDLER MANAJEMEN GRUP & TOPIK
# ==========================================

@dp.message(Command("set_backup"))
async def set_backup_group(message: Message):
    if message.chat.type not in ["group", "supergroup"]: 
        return await message.reply("⚠️ <b>Perhatian:</b>\nPerintah ini harus diketik langsung di dalam <b>Grup</b> yang ingin Anda jadikan Brankas Utama.", parse_mode="HTML")
    
    user_id = message.from_user.id
    user_res = await db_exec(lambda: supabase.table("users").select("role").eq("user_id", user_id).execute())
    if not user_res.data or user_res.data[0].get('role') != 'superadmin':
        return await message.reply("⛔ Hanya Super Admin yang berhak menetapkan Brankas Backup!")

    try:
        await db_exec(lambda: supabase.table("bot_settings").upsert({"setting_key": "backup_group_id", "setting_value": str(message.chat.id)}).execute())
        await db_exec(lambda: supabase.table("groups").upsert({"group_id": message.chat.id, "group_name": message.chat.title}).execute())
        await message.reply("✅ <b>BRANKAS UTAMA DITETAPKAN</b>\nGrup ini sekarang resmi menjadi pusat Disaster Recovery Center.", parse_mode="HTML")
    except Exception as e:
        await message.reply(f"❌ Gagal mengatur Brankas Utama: {e}")

@dp.message(Command("register_topic"))
async def register_topic(message: Message, command: CommandObject):
    if message.chat.type not in ["group", "supergroup"]: return
    member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
    if member.status not in ['creator', 'administrator']: return await message.reply("⛔ Hanya Admin Grup yang boleh mendaftarkan topik!")

    if not command.args: return await message.reply("⚠️ Gunakan: <code>/register_topic Nama Topik</code>", parse_mode="HTML")
    nama_topik, group_id, group_name, thread_id = command.args.strip(), message.chat.id, message.chat.title, message.message_thread_id or 0 
    
    try:
        await db_exec(lambda: supabase.table("groups").upsert({"group_id": group_id, "group_name": group_name}).execute())
        cek_topik = await db_exec(lambda: supabase.table("topics").select("*").eq("group_id", group_id).eq("message_thread_id", thread_id).execute())
        if len(cek_topik.data) > 0: return await message.reply(f"⚠️ Topik sudah ada: <b>{cek_topik.data[0]['topic_name']}</b>", parse_mode="HTML")
        await db_exec(lambda: supabase.table("topics").insert({"group_id": group_id, "message_thread_id": thread_id, "topic_name": nama_topik}).execute())
        await message.reply(f"✅ Topik <b>'{nama_topik}'</b> resmi terdaftar.", parse_mode="HTML")
    except Exception as e: await message.reply(f"❌ Error: {e}")

@dp.message(Command("group_settings"))
async def set_group_privacy(message: Message, command: CommandObject):
    if message.chat.type not in ["group", "supergroup"]: return
    member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
    if member.status not in ['creator', 'administrator']: return await message.reply("⛔ Hanya Admin Grup yang bisa mengatur privasi arsip!")

    if not command.args or command.args.strip().lower() not in ['on', 'off']:
        return await message.reply("⚠️ Format:\n<code>/group_settings on</code> (Izinkan member upload)\n<code>/group_settings off</code> (Hanya Admin yang boleh)", parse_mode="HTML")

    is_allowed = command.args.strip().lower() == 'on'
    try:
        await db_exec(lambda: supabase.table("groups").update({"allow_members": is_allowed}).eq("group_id", message.chat.id).execute())
        status_teks = "MEMBER DIIZINKAN" if is_allowed else "HANYA ADMIN"
        await message.reply(f"⚙️ <b>Privasi Grup Diperbarui:</b>\nSekarang pengiriman arsip ke grup ini: <b>{status_teks}</b>", parse_mode="HTML")
    except Exception as e: await message.reply(f"❌ Error: {e}")

# ==========================================
# 4. HANDLER UPLOAD MEDIA
# ==========================================

@dp.message(F.chat.type == "private", F.document | F.photo | F.video | F.audio | F.voice)
async def handle_private_media(message: Message):
    user_id = message.from_user.id
    if message.document: media_type, file_id, file_unique_id, nama_file = "document", message.document.file_id, message.document.file_unique_id, message.document.file_name
    elif message.photo: 
        media_type, file_id, file_unique_id = "photo", message.photo[-1].file_id, message.photo[-1].file_unique_id
        nama_file = f"Foto_{time.strftime('%Y%m%d_%H%M%S')}_{file_unique_id[-4:]}.jpg"
    elif message.video: media_type, file_id, file_unique_id, nama_file = "video", message.video.file_id, message.video.file_unique_id, message.video.file_name or "video.mp4"
    elif message.audio: media_type, file_id, file_unique_id, nama_file = "audio", message.audio.file_id, message.audio.file_unique_id, message.audio.file_name or "audio.mp3"
    elif message.voice: 
        media_type, file_id, file_unique_id = "voice", message.voice.file_id, message.voice.file_unique_id
        nama_file = f"Voice_{time.strftime('%Y%m%d_%H%M%S')}_{file_unique_id[-4:]}.ogg"

    if media_type == "document":
        ext = os.path.splitext(nama_file)[1].lower()
        if ext in BLACKLIST_EXT:
            await message.reply("⛔ <b>AKSES DITOLAK!</b>\nFormat file ini berbahaya dan dilarang masuk ke sistem arsip demi keamanan.", parse_mode="HTML")
            bg_res = await db_exec(lambda: supabase.table("bot_settings").select("setting_value").eq("setting_key", "backup_group_id").execute())
            if bg_res.data:
                backup_group_id_str = bg_res.data[0]['setting_value']
                warning_caption = f"🚨 <b>WARNING: DANGEROUS FILE BLOCKED</b>\nUser: @{message.from_user.username or message.from_user.id}\nFile: {html.escape(nama_file)}\n<i>File ditolak di antrean japri, tapi diamankan di sini sebagai bukti.</i>"
                try: await message.bot.send_document(chat_id=backup_group_id_str, document=file_id, caption=warning_caption, parse_mode="HTML")
                except Exception: pass
            return 

    try:
        await db_exec(lambda: supabase.table("upload_queue").insert({"user_id": user_id, "file_unique_id": file_unique_id, "file_id": file_id, "media_type": media_type, "original_name": nama_file, "status": "naming"}).execute())
        current_time = time.time()
        last_time = last_upload_time.get(user_id, 0)
        is_album = message.media_group_id is not None
        
        if is_album:
            if message.media_group_id not in album_cache:
                album_cache[message.media_group_id] = True
                await message.reply("📥 <b>Beberapa File Diterima!</b>\n<i>Silakan buka /queue untuk memprosesnya.</i>", parse_mode="HTML")
        else:
            if current_time - last_time < 1.5: pass 
            else:
                builder = InlineKeyboardBuilder()
                builder.button(text="➡️ Teruskan", callback_data=f"procq_{file_unique_id}")
                builder.button(text="❌ Hapus", callback_data=f"delq_{file_unique_id}")
                builder.adjust(2) 
                await message.reply(f"📥 <b>File Diterima!</b>\n📄 <code>{html.escape(nama_file)}</code>\n<i>Klik <b>Teruskan</b> atau buka /queue.</i>", reply_markup=builder.as_markup(), parse_mode="HTML")
        last_upload_time[user_id] = current_time
    except Exception as e: print(f"Error insert queue: {e}")

@dp.message(F.chat.type.in_(["group", "supergroup"]), F.document | F.photo | F.video | F.audio | F.voice)
async def handle_group_media(message: Message):
    source_group_id = message.chat.id
    source_thread_id = message.message_thread_id or 0

    bg_res = await db_exec(lambda: supabase.table("bot_settings").select("setting_value").eq("setting_key", "backup_group_id").execute())
    backup_group_id_str = bg_res.data[0]['setting_value'] if bg_res.data else None
    
    if backup_group_id_str and str(source_group_id) == backup_group_id_str: return 

    if message.document: media_type, file_id, file_unique_id, nama_file = "document", message.document.file_id, message.document.file_unique_id, message.document.file_name
    elif message.photo: 
        media_type, file_id, file_unique_id = "photo", message.photo[-1].file_id, message.photo[-1].file_unique_id
        nama_file = f"Foto_{time.strftime('%Y%m%d_%H%M%S')}_{file_unique_id[-4:]}.jpg"
    elif message.video: media_type, file_id, file_unique_id, nama_file = "video", message.video.file_id, message.video.file_unique_id, message.video.file_name or "video.mp4"
    elif message.audio: media_type, file_id, file_unique_id, nama_file = "audio", message.audio.file_id, message.audio.file_unique_id, message.audio.file_name or "audio.mp3"
    elif message.voice: 
        media_type, file_id, file_unique_id = "voice", message.voice.file_id, message.voice.file_unique_id
        nama_file = f"Voice_{time.strftime('%Y%m%d_%H%M%S')}_{file_unique_id[-4:]}.ogg"

    msg_id = message.message_id

    if media_type == "document":
        ext = os.path.splitext(nama_file)[1].lower()
        if ext in BLACKLIST_EXT:
            try: await message.delete()
            except Exception: pass
            
            if backup_group_id_str:
                warning_caption = f"🚨 <b>WARNING: DANGEROUS FILE AUTO-DELETED!</b>\nGrup Asal: {message.chat.title}\nUser: @{message.from_user.username or message.from_user.id}\nFile: {html.escape(nama_file)}\n<i>File berbahaya ini telah dihapus dari grup asalnya, dan diamankan di sini sebagai bukti.</i>"
                try: await message.bot.send_document(chat_id=backup_group_id_str, document=file_id, caption=warning_caption, parse_mode="HTML")
                except Exception: pass
            return 

    if backup_group_id_str:
        try:
            map_res = await db_exec(lambda: supabase.table("backup_mapping").select("backup_thread_id").eq("source_group_id", source_group_id).execute())
            backup_thread_id = None
            if map_res.data: backup_thread_id = map_res.data[0]['backup_thread_id']
            else:
                try:
                    new_topic = await message.bot.create_forum_topic(chat_id=backup_group_id_str, name=message.chat.title)
                    backup_thread_id = new_topic.message_thread_id
                    await db_exec(lambda: supabase.table("backup_mapping").insert({"source_group_id": source_group_id, "backup_thread_id": backup_thread_id}).execute())
                except Exception as e: print(f"Gagal bikin topik backup: {e}")

            if backup_thread_id:
                sender = message.from_user
                sender_name = f"@{sender.username}" if sender.username else sender.full_name
                topic_name = "General / Tidak Terdaftar"
                if source_thread_id != 0:
                    cek_topik_asal = await db_exec(lambda: supabase.table("topics").select("topic_name").eq("group_id", source_group_id).eq("message_thread_id", source_thread_id).execute())
                    if cek_topik_asal.data: topic_name = cek_topik_asal.data[0]['topic_name']

                caption = f"📁 <b>{html.escape(nama_file)}</b>\n🏢 Asal: {message.chat.title} (Topik: {topic_name})\n👤 Pengirim: {sender_name}"

                if media_type == "document": await message.bot.send_document(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, document=file_id, caption=caption, parse_mode="HTML")
                elif media_type == "photo": await message.bot.send_photo(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, photo=file_id, caption=caption, parse_mode="HTML")
                elif media_type == "video": await message.bot.send_video(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, video=file_id, caption=caption, parse_mode="HTML")
                elif media_type == "audio": await message.bot.send_audio(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, audio=file_id, caption=caption, parse_mode="HTML")
                elif media_type == "voice": await message.bot.send_voice(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, voice=file_id, caption=caption, parse_mode="HTML")
        except Exception as e: print(f"CCTV Backup Error: {e}")

    cek_topik = await db_exec(lambda: supabase.table("topics").select("topic_name").eq("group_id", source_group_id).eq("message_thread_id", source_thread_id).execute())
    if cek_topik.data:
        try:
            await db_exec(lambda: supabase.table("files").upsert({
                "file_unique_id": file_unique_id, "file_id": file_id, "display_name": nama_file, 
                "media_type": media_type, "group_id": source_group_id, "message_thread_id": source_thread_id, "message_id": msg_id
            }).execute())
        except Exception: pass

# ==========================================
# 5. HANDLER TOMBOL UI
# ==========================================

@dp.callback_query(F.data == "list_groups")
async def call_list_groups(callback: CallbackQuery):
    try:
        await callback.message.delete()
        teks, markup = await get_list_groups_ui(callback.bot, callback.from_user.id)
        await callback.message.answer(teks, reply_markup=markup, parse_mode="HTML")
    except Exception: await callback.answer("Gagal membuka grup.")

@dp.callback_query(F.data.startswith("lgrp_"))
async def call_list_topics(callback: CallbackQuery):
    g_id = callback.data.replace("lgrp_", "")
    try:
        await callback.message.delete()
        teks, markup = await get_list_topics_ui(g_id)
        await callback.message.answer(teks, reply_markup=markup, parse_mode="HTML")
    except Exception: await callback.answer("Gagal membuka topik.")

@dp.callback_query(F.data.startswith("ltop_"))
async def call_list_files(callback: CallbackQuery):
    g_id, t_id = callback.data.replace("ltop_", "").split("_")
    try:
        await callback.message.delete()
        teks, markup = await get_list_files_ui(g_id, t_id, page=0)
        await callback.message.answer(teks, reply_markup=markup, parse_mode="HTML")
    except Exception: await callback.answer("Gagal memuat file.")

@dp.callback_query(F.data.startswith("lpage_"))
async def call_list_files_page(callback: CallbackQuery):
    g_id, t_id, page = callback.data.replace("lpage_", "").split("_")
    try:
        teks, markup = await get_list_files_ui(g_id, t_id, page=int(page))
        await callback.message.edit_text(teks, reply_markup=markup, parse_mode="HTML")
    except Exception: await callback.answer("Gagal pindah halaman.")

@dp.callback_query(F.data.startswith("fnum_"))
async def preview_hasil_files(callback: CallbackQuery):
    fid = callback.data.replace("fnum_", "")
    try:
        response = await db_exec(lambda: supabase.table("files").select("*").eq("file_unique_id", fid).execute())
        if not response.data: return await callback.answer("File arsip tidak ditemukan!", show_alert=True)
            
        item = response.data[0]
        teks = f"📂 <b>{html.escape(item['display_name'])}</b>"
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 Pindahkan", callback_data=f"mv_{fid}")
        builder.button(text="📑 Salin", callback_data=f"cp_{fid}")
        builder.button(text="🔙 Kembali", callback_data=f"backl_{fid}")
        builder.adjust(2, 1)
        
        await callback.message.delete()
        if item['media_type'] == "document": await callback.message.answer_document(document=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "photo": await callback.message.answer_photo(photo=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "video": await callback.message.answer_video(video=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "audio": await callback.message.answer_audio(audio=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "voice": await callback.message.answer_voice(voice=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
    except Exception: await callback.answer(f"Gagal memuat arsip.", show_alert=True)

@dp.callback_query(F.data.startswith("backl_"))
async def kembali_ke_list_files(callback: CallbackQuery):
    fid = callback.data.replace("backl_", "")
    try:
        res = await db_exec(lambda: supabase.table("files").select("group_id, message_thread_id").eq("file_unique_id", fid).execute())
        if res.data:
            await callback.message.delete()
            teks, markup = await get_list_files_ui(res.data[0]['group_id'], res.data[0]['message_thread_id'], page=0)
            await callback.message.answer(teks, reply_markup=markup, parse_mode="HTML")
        else:
            await callback.message.delete()
            teks, markup = await get_list_groups_ui(callback.bot, callback.from_user.id)
            await callback.message.answer(teks, reply_markup=markup, parse_mode="HTML")
    except Exception: await callback.answer("Gagal kembali.")

@dp.callback_query(F.data.startswith("spage_"))
async def ganti_halaman_search(callback: CallbackQuery):
    page = int(callback.data.replace("spage_", ""))
    try:
        teks, markup = await get_search_ui(callback.bot, callback.from_user.id, page)
        await callback.message.edit_text(teks, reply_markup=markup, parse_mode="HTML")
    except Exception: await callback.answer("Gagal pindah halaman.", show_alert=True)

@dp.callback_query(F.data.startswith("snum_"))
async def preview_hasil_search(callback: CallbackQuery):
    fid = callback.data.replace("snum_", "")
    try:
        response = await db_exec(lambda: supabase.table("files").select("*").eq("file_unique_id", fid).execute())
        if not response.data: return await callback.answer("File tidak ditemukan!", show_alert=True)
            
        item = response.data[0]
        teks = f"📂 <b>{html.escape(item['display_name'])}</b>"
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 Pindahkan", callback_data=f"mv_{fid}")
        builder.button(text="📑 Salin", callback_data=f"cp_{fid}")
        builder.button(text="🔙 Kembali Cari", callback_data="backs")
        builder.adjust(2, 1)
        
        await callback.message.delete()
        if item['media_type'] == "document": await callback.message.answer_document(document=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "photo": await callback.message.answer_photo(photo=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "video": await callback.message.answer_video(video=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "audio": await callback.message.answer_audio(audio=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "voice": await callback.message.answer_voice(voice=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
    except Exception: await callback.answer(f"Gagal memuat arsip.", show_alert=True)

@dp.callback_query(F.data == "backs")
async def kembali_ke_search(callback: CallbackQuery):
    try:
        await callback.message.delete()
        teks, markup = await get_search_ui(callback.bot, callback.from_user.id)
        await callback.message.answer(teks, reply_markup=markup, parse_mode="HTML") if markup else await callback.message.answer(teks, parse_mode="HTML")
    except Exception: await callback.answer("Gagal kembali.")

@dp.callback_query(F.data.startswith("mv_") | F.data.startswith("cp_"))
async def action_pilih_grup(callback: CallbackQuery):
    action, fid = callback.data.split("_", 1)
    allowed_groups = await get_allowed_groups(callback.bot, callback.from_user.id)
    if not allowed_groups: return await callback.answer("Anda tidak memiliki akses ke grup manapun!", show_alert=True)
        
    builder = InlineKeyboardBuilder()
    for grup in allowed_groups:
        builder.button(text=f"🏢 {grup['group_name']}", callback_data=f"{action}g_{grup['group_id']}_{fid}")
    builder.button(text="❌ Batal (Tutup)", callback_data="delete_msg")
    builder.adjust(1)
    
    judul = "🔄 <b>Pindah Lokasi File</b>" if action == "mv" else "📑 <b>Salin File</b>"
    await callback.message.delete()
    await callback.message.answer(f"{judul}\nPilih <b>Grup</b> tujuan:", reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("mvg_") | F.data.startswith("cpg_"))
async def action_pilih_topik(callback: CallbackQuery):
    parts = callback.data.split("_")
    action_type = parts[0]
    group_id_str, fid = parts[1], parts[2]
    
    data_topik = await db_exec(lambda: supabase.table("topics").select("*").eq("group_id", group_id_str).execute())
    original_action = "mv" if action_type == "mvg" else "cp"

    builder = InlineKeyboardBuilder()
    if not data_topik.data: 
        builder.button(text="🔙 Kembali Pilih Grup", callback_data=f"{original_action}_{fid}")
        builder.adjust(1)
        return await callback.message.edit_text("⚠️ Grup ini belum punya Topik. Silakan pilih grup lain.", reply_markup=builder.as_markup(), parse_mode="HTML")

    for topik in data_topik.data:
        next_action = "mvt" if action_type == "mvg" else "cpt"
        builder.button(text=f"📂 {topik['topic_name']}", callback_data=f"{next_action}_{topik['message_thread_id']}_{group_id_str}_{fid}")
    
    builder.button(text="🔙 Kembali Pilih Grup", callback_data=f"{original_action}_{fid}")
    builder.adjust(1) 
    
    judul = "🔄 <b>Pindah Lokasi File</b>" if action_type == "mvg" else "📑 <b>Salin File</b>"
    await callback.message.edit_text(f"{judul}\n🏢 Grup dipilih.\n\nPilih <b>Topik (Folder)</b> tujuan:", reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("mvt_") | F.data.startswith("cpt_"))
async def action_eksekusi(callback: CallbackQuery):
    parts = callback.data.split("_")
    action_type = parts[0]
    thread_id_str, group_id_str, fid = parts[1], parts[2], parts[3]
    msg_thread_id = int(thread_id_str) if int(thread_id_str) != 0 else None

    is_move = action_type == "mvt"
    await callback.message.edit_text("⏳ <i>Sedang memproses...</i>", parse_mode="HTML")

    try:
        file_data = await db_exec(lambda: supabase.table("files").select("*").eq("file_unique_id", fid).execute())
        if not file_data.data: return await callback.message.edit_text("❌ File tidak ditemukan.")
        
        item = file_data.data[0]
        caption = f"📁 <b>{html.escape(item['display_name'])}</b>"
        media_type, file_id, old_group_id, old_message_id = item['media_type'], item['file_id'], item['group_id'], item.get('message_id')

        if is_move and old_message_id:
            try: await callback.bot.delete_message(chat_id=old_group_id, message_id=old_message_id)
            except Exception: pass

        sent_msg = None
        if media_type == "document": sent_msg = await callback.bot.send_document(chat_id=group_id_str, message_thread_id=msg_thread_id, document=file_id, caption=caption, parse_mode="HTML")
        elif media_type == "photo": sent_msg = await callback.bot.send_photo(chat_id=group_id_str, message_thread_id=msg_thread_id, photo=file_id, caption=caption, parse_mode="HTML")
        elif media_type == "video": sent_msg = await callback.bot.send_video(chat_id=group_id_str, message_thread_id=msg_thread_id, video=file_id, caption=caption, parse_mode="HTML")
        elif media_type == "audio": sent_msg = await callback.bot.send_audio(chat_id=group_id_str, message_thread_id=msg_thread_id, audio=file_id, caption=caption, parse_mode="HTML")
        elif media_type == "voice": sent_msg = await callback.bot.send_voice(chat_id=group_id_str, message_thread_id=msg_thread_id, voice=file_id, caption=caption, parse_mode="HTML")

        new_msg_id = sent_msg.message_id if sent_msg else None

        if is_move:
            await db_exec(lambda: supabase.table("files").update({"group_id": group_id_str, "message_thread_id": int(thread_id_str), "message_id": new_msg_id}).eq("file_unique_id", fid).execute())
            await callback.message.edit_text(f"🎉 <b>BERHASIL DIPINDAHKAN!</b>\n\nFile <b>{html.escape(item['display_name'])}</b> berada di lokasi baru.", parse_mode="HTML")
        else:
            new_fid = f"{fid}_copy_{int(time.time())}"
            await db_exec(lambda: supabase.table("files").insert({"file_unique_id": new_fid, "file_id": file_id, "display_name": item['display_name'], "media_type": media_type, "group_id": group_id_str, "message_thread_id": int(thread_id_str), "message_id": new_msg_id}).execute())
            await callback.message.edit_text(f"🎉 <b>BERHASIL DISALIN!</b>\n\nFile <b>{html.escape(item['display_name'])}</b> berhasil digandakan.", parse_mode="HTML")

    except Exception as e: 
        err_msg = str(e).lower()
        if "thread not found" in err_msg or "topic not found" in err_msg:
            await db_exec(lambda: supabase.table("topics").delete().eq("message_thread_id", int(thread_id_str)).eq("group_id", group_id_str).execute())
            return await callback.message.edit_text("❌ <b>GAGAL: TOPIK SUDAH TIDAK ADA!</b>\n\nTopik tujuan ternyata sudah dihapus manual. Silakan ulangi prosesnya.", parse_mode="HTML")
        elif "chat not found" in err_msg or "bot was kicked" in err_msg or "forbidden" in err_msg:
            await db_exec(lambda: supabase.table("groups").delete().eq("group_id", group_id_str).execute())
            await db_exec(lambda: supabase.table("topics").delete().eq("group_id", group_id_str).execute())
            return await callback.message.edit_text("❌ <b>GAGAL: GRUP SUDAH TIDAK ADA/AKSES DITOLAK!</b>", parse_mode="HTML")
        await callback.message.edit_text(f"❌ <b>Gagal memproses aksi!</b>\nError: <code>{e}</code>\n\n<i>File tidak dimasukkan ke database tujuan.</i>", parse_mode="HTML")

@dp.callback_query(F.data == "delete_msg")
async def delete_bot_message(callback: CallbackQuery):
    try: await callback.message.delete()
    except: pass

@dp.callback_query(F.data == "clear_all_queue")
async def clear_all_queue(callback: CallbackQuery):
    user_id = callback.from_user.id
    try:
        await db_exec(lambda: supabase.table("upload_queue").delete().eq("user_id", user_id).execute())
        await callback.message.edit_text("🧹 <b>Semua antrean berhasil dikosongkan!</b>", parse_mode="HTML")
    except Exception: await callback.answer("Gagal mengosongkan antrean.", show_alert=True)

@dp.callback_query(F.data.startswith("qpage_"))
async def ganti_halaman_antrean(callback: CallbackQuery):
    page = int(callback.data.replace("qpage_", ""))
    try:
        teks, markup = await get_queue_ui(callback.from_user.id, page)
        await callback.message.edit_text(teks, reply_markup=markup, parse_mode="HTML")
    except Exception: await callback.answer("Gagal pindah halaman.", show_alert=True)

@dp.callback_query(F.data.startswith("qnum_"))
async def menu_detail_antrean(callback: CallbackQuery):
    fid, user_id = callback.data.replace("qnum_", ""), callback.from_user.id
    try:
        response = await db_exec(lambda: supabase.table("upload_queue").select("*").eq("file_unique_id", fid).eq("user_id", user_id).execute())
        if not response.data: return await callback.answer("File tidak ditemukan!", show_alert=True)
            
        item = response.data[0]
        teks = f"📄 <b>PREVIEW FILE</b>\n\n<b>Nama Asli:</b> <code>{html.escape(item['original_name'])}</code>\n<b>Status:</b> <i>{'Menunggu Diproses' if item['status'] in ['naming','active_naming'] else 'Menunggu Grup/Topik'}</i>"
        
        builder = InlineKeyboardBuilder()
        builder.button(text="➡️ Proses File Ini", callback_data=f"procq_{fid}")
        builder.button(text="❌ Hapus", callback_data=f"delq_{fid}")
        builder.button(text="🔙 Kembali ke Daftar", callback_data="backq")
        builder.adjust(2, 1)
        
        await callback.message.delete()
        if item['media_type'] == "document": await callback.message.answer_document(document=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "photo": await callback.message.answer_photo(photo=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "video": await callback.message.answer_video(video=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "audio": await callback.message.answer_audio(audio=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        elif item['media_type'] == "voice": await callback.message.answer_voice(voice=item['file_id'], caption=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
    except Exception: await callback.answer(f"Gagal memuat detail file.", show_alert=True)

@dp.callback_query(F.data == "backq")
async def kembali_ke_antrean(callback: CallbackQuery):
    try:
        await callback.message.delete()
        teks, markup = await get_queue_ui(callback.from_user.id)
        await callback.message.answer(teks, reply_markup=markup, parse_mode="HTML") if markup else await callback.message.answer(teks, parse_mode="HTML")
    except Exception: await callback.answer("Gagal kembali.")


# ==========================================
# WIZARD FLOW INTERAKTIF (Ganti Nama & Caption)
# ==========================================

async def proceed_queue_logic(callback: CallbackQuery, fid: str, user_id: int):
    try:
        response = await db_exec(lambda: supabase.table("upload_queue").select("*").eq("file_unique_id", fid).eq("user_id", user_id).execute())
        if not response.data: 
            return await callback.answer("File tidak ditemukan atau sudah diproses!", show_alert=True)
            
        antrean = response.data[0]
        
        builder = InlineKeyboardBuilder()
        builder.button(text="✏️ Ganti Nama", callback_data=f"askrn_yes_{fid}")
        builder.button(text="⏭️ Skip", callback_data=f"askrn_no_{fid}")
        builder.adjust(2)
        
        # Hapus pesan lama secara aman
        try: await callback.message.delete()
        except: pass
        
        safe_name = html.escape(antrean.get('original_name') or 'Unknown_File')
        teks = f"📝 <b>Langkah 1: Ganti Nama</b>\nFile Asli: <code>{safe_name}</code>\n\nMau ganti nama file ini sebelum disimpan?"
        
        # 100% AMAN: Menggunakan bot.send_message langsung ke ID user, mengabaikan status pesan lama yang sudah dihapus
        await callback.bot.send_message(chat_id=user_id, text=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
        
    except Exception as e:
        print(f"Error proceed_queue: {e}")
        await callback.bot.send_message(chat_id=user_id, text=f"❌ <b>Error Langkah 1:</b>\n<code>{e}</code>", parse_mode="HTML")

@dp.callback_query(F.data.startswith("askrn_yes_"))
async def wizard_rename_yes(callback: CallbackQuery, state: FSMContext):
    fid = callback.data.replace("askrn_yes_", "")
    await state.set_state(WizardState.waiting_for_rename)
    await state.update_data(current_fid=fid)
    await callback.message.edit_text("Silakan balas pesan ini dengan <b>Nama Baru</b> untuk file tersebut:", parse_mode="HTML")

@dp.message(WizardState.waiting_for_rename, F.text)
async def receive_wizard_rename(message: Message, state: FSMContext):
    data = await state.get_data()
    fid = data.get("current_fid")
    user_id = message.from_user.id
    nama_final = message.text
    
    await db_exec(lambda: supabase.table("upload_queue").update({"display_name": nama_final}).eq("file_unique_id", fid).eq("user_id", user_id).execute())
    await state.clear()
    await ask_caption_visibility(message, fid)

@dp.callback_query(F.data.startswith("askrn_no_"))
async def wizard_rename_no(callback: CallbackQuery):
    fid = callback.data.replace("askrn_no_", "")
    user_id = callback.from_user.id
    
    res = await db_exec(lambda: supabase.table("upload_queue").select("original_name").eq("file_unique_id", fid).eq("user_id", user_id).execute())
    if res.data:
        nama_asli = res.data[0]['original_name']
        await db_exec(lambda: supabase.table("upload_queue").update({"display_name": nama_asli}).eq("file_unique_id", fid).eq("user_id", user_id).execute())
    
    await ask_caption_visibility(callback.message, fid, is_callback=True)

async def ask_caption_visibility(message: Message, fid: str, is_callback=False):
    builder = InlineKeyboardBuilder()
    builder.button(text="👁️ Tampilkan", callback_data=f"capshow_yes_{fid}")
    builder.button(text="🚫 Engga", callback_data=f"capshow_no_{fid}")
    builder.adjust(2)
    teks = "📝 <b>Langkah 2: Tampilan Caption</b>\nApakah Anda ingin menampilkan detail nama file di Caption grup tujuan?"
    
    if is_callback:
        try: await message.delete()
        except: pass
        await message.bot.send_message(chat_id=message.chat.id, text=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await message.answer(teks, reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("capshow_"))
async def handle_caption_visibility(callback: CallbackQuery):
    choice, fid = callback.data.replace("capshow_", "").split("_", 1)
    wizard_cache.setdefault(fid, {})['show_name'] = (choice == "yes")
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✍️ Tambah", callback_data=f"addcap_yes_{fid}")
    builder.button(text="⏭️ Skip", callback_data=f"addcap_no_{fid}")
    builder.adjust(2)
    await callback.message.edit_text("📝 <b>Langkah 3: Caption Tambahan</b>\nMau nambahin caption teks manual di bawah file ini?", reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("addcap_yes_"))
async def ask_custom_caption(callback: CallbackQuery, state: FSMContext):
    fid = callback.data.replace("addcap_yes_", "")
    await state.set_state(WizardState.waiting_for_caption)
    await state.update_data(current_fid=fid)
    await callback.message.edit_text("Silakan balas pesan ini dengan <b>Caption Tambahan</b> Anda:", parse_mode="HTML")

@dp.message(WizardState.waiting_for_caption, F.text)
async def receive_custom_caption(message: Message, state: FSMContext):
    data = await state.get_data()
    fid = data.get("current_fid")
    wizard_cache.setdefault(fid, {})['custom_cap'] = message.text
    await state.clear()
    await select_destination_group(message, fid, message.from_user.id)

@dp.callback_query(F.data.startswith("addcap_no_"))
async def skip_custom_caption(callback: CallbackQuery):
    fid = callback.data.replace("addcap_no_", "")
    wizard_cache.setdefault(fid, {})['custom_cap'] = ""
    await select_destination_group(callback.message, fid, callback.from_user.id, is_callback=True)

async def select_destination_group(message: Message, fid: str, user_id: int, is_callback=False):
    allowed_groups = await get_allowed_groups(message.bot if is_callback else message.bot, user_id)
    if not allowed_groups: 
        teks = "⚠️ Anda tidak memiliki akses grup arsip manapun."
        if is_callback: await message.edit_text(teks, parse_mode="HTML")
        else: await message.answer(teks, parse_mode="HTML")
        return
        
    builder = InlineKeyboardBuilder()
    for grup in allowed_groups: builder.button(text=f"🏢 {grup['group_name']}", callback_data=f"grup_{grup['group_id']}_{fid}")
    builder.adjust(1)
    
    await db_exec(lambda: supabase.table("upload_queue").update({"status": "selecting_topic"}).eq("file_unique_id", fid).eq("user_id", user_id).execute())
    
    teks = "🎯 <b>Langkah Terakhir:</b>\nPilih <b>Grup Tujuan</b> penyimpanan arsip:"
    if is_callback:
        try: await message.delete()
        except: pass
        await message.bot.send_message(chat_id=user_id, text=teks, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await message.answer(teks, reply_markup=builder.as_markup(), parse_mode="HTML")


# ==========================================
# FLOW UTAMA PROSES ANTREAN & DUPLIKAT
# ==========================================
@dp.callback_query(F.data.startswith("procq_"))
async def proses_antrean(callback: CallbackQuery):
    fid = callback.data.replace("procq_", "")
    user_id = callback.from_user.id
    
    try: await callback.answer()
    except: pass
    
    try:
        cek_dup = await db_exec(lambda: supabase.table("files").select("*").eq("file_unique_id", fid).execute())
        if cek_dup.data:
            dup = cek_dup.data[0]
            g_res = await db_exec(lambda: supabase.table("groups").select("group_name").eq("group_id", dup['group_id']).execute())
            g_name = g_res.data[0]['group_name'] if g_res.data else "Grup Tidak Diketahui"
            
            t_name = "General / Tidak Terdaftar"
            if dup.get('message_thread_id') and dup['message_thread_id'] != 0:
                t_res = await db_exec(lambda: supabase.table("topics").select("topic_name").eq("group_id", dup['group_id']).eq("message_thread_id", dup['message_thread_id']).execute())
                if t_res.data: t_name = t_res.data[0]['topic_name']

            teks_dup = f"⚠️ <b>DUPLIKAT TERDETEKSI!</b>\n\nFile ini persis dengan yang sudah pernah lu kirim ke:\n🏢 <b>{g_name}</b>\n📂 Topik: <b>{t_name}</b>\n\n<i>Kalo lu lanjut, file ini bakal dicopy/digandakan. Mau tetep lanjut?</i>"
            
            builder = InlineKeyboardBuilder()
            builder.button(text="✅ Lanjut (Gandakan)", callback_data=f"forceq_{fid[-15:]}") 
            builder.button(text="❌ Nggak Jadi", callback_data=f"delq_{fid}")
            builder.adjust(1)
            
            try: await callback.message.delete()
            except: pass
            
            album_cache[f"dup_{user_id}"] = fid
            return await callback.bot.send_message(chat_id=user_id, text=teks_dup, reply_markup=builder.as_markup(), parse_mode="HTML")

        await proceed_queue_logic(callback, fid, user_id)
        
    except Exception as e: 
        print(f"Error procq: {e}")
        await callback.bot.send_message(chat_id=user_id, text=f"❌ <b>Error Utama:</b>\n<code>{e}</code>", parse_mode="HTML")

@dp.callback_query(F.data.startswith("forceq_"))
async def force_proses_antrean(callback: CallbackQuery):
    user_id = callback.from_user.id
    fid = album_cache.get(f"dup_{user_id}")
    
    try: await callback.answer()
    except: pass
    
    if not fid: return await callback.bot.send_message(chat_id=user_id, text="Sesi kadaluarsa, silakan ulang dari /queue")
        
    try:
        q_res = await db_exec(lambda: supabase.table("upload_queue").select("*").eq("file_unique_id", fid).eq("user_id", user_id).execute())
        if not q_res.data: return await callback.bot.send_message(chat_id=user_id, text="Antrean tidak ditemukan!")
        
        old_item = q_res.data[0]
        new_fid = f"c{int(time.time() % 100000)}_{fid[-5:]}"
        
        await db_exec(lambda: supabase.table("upload_queue").insert({
            "user_id": user_id, 
            "file_unique_id": new_fid, 
            "file_id": old_item['file_id'],
            "media_type": old_item['media_type'], 
            "original_name": old_item['original_name'],
            "display_name": old_item.get('display_name'), 
            "status": old_item['status'], 
            "group_id": old_item.get('group_id')
        }).execute())
        
        await db_exec(lambda: supabase.table("upload_queue").delete().eq("file_unique_id", fid).eq("user_id", user_id).execute())
        
        try: await callback.message.delete()
        except: pass
        
        await proceed_queue_logic(callback, new_fid, user_id)
    except Exception as e:
        print(f"Error forceq: {e}")
        await callback.bot.send_message(chat_id=user_id, text=f"❌ <b>Error Gandakan:</b>\n<code>{e}</code>", parse_mode="HTML")

@dp.callback_query(F.data.startswith("delq_"))
async def hapus_antrean(callback: CallbackQuery):
    fid, user_id = callback.data.replace("delq_", ""), callback.from_user.id
    try:
        await db_exec(lambda: supabase.table("upload_queue").delete().eq("file_unique_id", fid).eq("user_id", user_id).execute())
        try: await callback.message.delete()
        except: pass
        teks, markup = await get_queue_ui(user_id)
        await callback.message.answer(teks, reply_markup=markup, parse_mode="HTML") if markup else await callback.message.answer(teks, parse_mode="HTML")
    except Exception: await callback.answer("Gagal menghapus", show_alert=True)

@dp.callback_query(F.data.startswith("grup_"))
async def pilih_grup(callback: CallbackQuery):
    group_id_str, fid = callback.data[5:].split("_", 1)
    user_id = callback.from_user.id
    try:
        await db_exec(lambda: supabase.table("upload_queue").update({"group_id": group_id_str}).eq("file_unique_id", fid).eq("user_id", user_id).execute())
        data_topik = await db_exec(lambda: supabase.table("topics").select("*").eq("group_id", group_id_str).execute())
        
        builder = InlineKeyboardBuilder()
        if not data_topik.data: 
            builder.button(text="🔙 Kembali Pilih Grup", callback_data=f"procq_{fid}")
            builder.adjust(1)
            return await callback.message.edit_text("⚠️ Grup ini belum punya Topik. Pilih grup lain!", reply_markup=builder.as_markup(), parse_mode="HTML")

        for topik in data_topik.data: builder.button(text=f"📂 {topik['topic_name']}", callback_data=f"topik_{topik['message_thread_id']}_{fid}")
        builder.button(text="🔙 Kembali Pilih Grup", callback_data=f"procq_{fid}")
        builder.adjust(1) 
        
        try: await callback.message.delete()
        except: pass
        
        await callback.bot.send_message(chat_id=user_id, text="Mantap! 🏢 Grup dipilih.\n\nSekarang, pilih <b>Topik (Folder)</b> tujuannya:", reply_markup=builder.as_markup(), parse_mode="HTML")
    except Exception: await callback.answer("Gagal memproses", show_alert=True)

# ==========================================
# FITUR BULK & SINGLE (SAFE DELETE)
# ==========================================
@dp.callback_query(F.data == "bulk_start")
async def bulk_start(callback: CallbackQuery):
    allowed_groups = await get_allowed_groups(callback.bot, callback.from_user.id)
    if not allowed_groups: return await callback.answer("Anda tidak memiliki akses ke grup arsip manapun!", show_alert=True)
        
    builder = InlineKeyboardBuilder()
    for grup in allowed_groups: builder.button(text=f"🏢 {grup['group_name']}", callback_data=f"bulkgrup_{grup['group_id']}")
    builder.button(text="🔙 Batal", callback_data="backq")
    builder.adjust(1)
    await callback.message.edit_text("📦 <b>PROSES MASSAL</b>\n\nMau dikirim ke <b>Grup</b> mana?", reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("bulkgrup_"))
async def bulk_pilih_grup(callback: CallbackQuery):
    group_id_str = callback.data.replace("bulkgrup_", "")
    data_topik = await db_exec(lambda: supabase.table("topics").select("*").eq("group_id", group_id_str).execute())
    
    builder = InlineKeyboardBuilder()
    if not data_topik.data: 
        builder.button(text="🔙 Kembali Pilih Grup", callback_data="bulk_start")
        builder.adjust(1)
        return await callback.message.edit_text("⚠️ Grup ini belum punya Topik. Pilih grup lain!", reply_markup=builder.as_markup(), parse_mode="HTML")

    for topik in data_topik.data: builder.button(text=f"📂 {topik['topic_name']}", callback_data=f"bulktopik_{topik['message_thread_id']}_{group_id_str}")
    builder.button(text="🔙 Kembali Pilih Grup", callback_data="bulk_start")
    builder.adjust(1) 
    await callback.message.edit_text("📦 <b>PROSES MASSAL</b>\nSekarang, pilih <b>Topik (Folder)</b> tujuannya:", reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("bulktopik_"))
async def bulk_eksekusi(callback: CallbackQuery):
    user_id = callback.from_user.id
    thread_id_str, group_id_str = callback.data[10:].split("_", 1)
    msg_thread_id = int(thread_id_str) if int(thread_id_str) != 0 else None

    await callback.message.edit_text("⏳ <i>Sedang memproses dan mengirim semua file...</i>", parse_mode="HTML")

    bg_res = await db_exec(lambda: supabase.table("bot_settings").select("setting_value").eq("setting_key", "backup_group_id").execute())
    backup_group_id_str = bg_res.data[0]['setting_value'] if bg_res.data else None

    g_res = await db_exec(lambda: supabase.table("groups").select("group_name").eq("group_id", int(group_id_str)).execute())
    g_name = g_res.data[0]['group_name'] if g_res.data else "Grup"
    
    t_name = "General / Tidak Terdaftar"
    if msg_thread_id:
        t_res = await db_exec(lambda: supabase.table("topics").select("topic_name").eq("group_id", int(group_id_str)).eq("message_thread_id", msg_thread_id).execute())
        if t_res.data: t_name = t_res.data[0]['topic_name']

    sender_name = f"@{callback.from_user.username}" if callback.from_user.username else callback.from_user.full_name

    try:
        antrean_list = await db_exec(lambda: supabase.table("upload_queue").select("*").eq("user_id", user_id).execute())
        sukses = 0
        berhasil_fids = []
        
        for item in antrean_list.data:
            fid, file_id, media_type = item['file_unique_id'], item['file_id'], item['media_type']
            
            cek_dup = await db_exec(lambda: supabase.table("files").select("file_unique_id").eq("file_unique_id", fid).execute())
            if cek_dup.data: fid = f"c{int(time.time() % 100000)}_{fid[-5:]}"

            display_name = item['display_name'] if item['display_name'] else item['original_name']
            safe_display_name = html.escape(display_name)
            caption = f"📁 <b>{safe_display_name}</b>"

            try:
                sent_msg = None
                if media_type == "document": sent_msg = await callback.bot.send_document(chat_id=group_id_str, message_thread_id=msg_thread_id, document=file_id, caption=caption, parse_mode="HTML")
                elif media_type == "photo": sent_msg = await callback.bot.send_photo(chat_id=group_id_str, message_thread_id=msg_thread_id, photo=file_id, caption=caption, parse_mode="HTML")
                elif media_type == "video": sent_msg = await callback.bot.send_video(chat_id=group_id_str, message_thread_id=msg_thread_id, video=file_id, caption=caption, parse_mode="HTML")
                elif media_type == "audio": sent_msg = await callback.bot.send_audio(chat_id=group_id_str, message_thread_id=msg_thread_id, audio=file_id, caption=caption, parse_mode="HTML")
                elif media_type == "voice": sent_msg = await callback.bot.send_voice(chat_id=group_id_str, message_thread_id=msg_thread_id, voice=file_id, caption=caption, parse_mode="HTML")

                msg_id = sent_msg.message_id if sent_msg else None
                await db_exec(lambda: supabase.table("files").insert({"file_unique_id": fid, "file_id": file_id, "display_name": display_name, "media_type": media_type, "group_id": group_id_str, "message_thread_id": int(thread_id_str), "message_id": msg_id}).execute())
                
                sukses += 1
                berhasil_fids.append(item['file_unique_id']) 

                if backup_group_id_str and str(group_id_str) != backup_group_id_str:
                    try:
                        map_res = await db_exec(lambda: supabase.table("backup_mapping").select("backup_thread_id").eq("source_group_id", int(group_id_str)).execute())
                        backup_thread_id = None
                        if map_res.data: backup_thread_id = map_res.data[0]['backup_thread_id']
                        else:
                            new_topic = await callback.bot.create_forum_topic(chat_id=backup_group_id_str, name=g_name)
                            backup_thread_id = new_topic.message_thread_id
                            await db_exec(lambda: supabase.table("backup_mapping").insert({"source_group_id": int(group_id_str), "backup_thread_id": backup_thread_id}).execute())
                        
                        caption_backup = f"📁 <b>{safe_display_name}</b>\n🏢 Asal: {g_name} (Topik: {t_name})\n👤 Pengirim: {sender_name} (Via Bot Japri)"
                        
                        if media_type == "document": await callback.bot.send_document(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, document=file_id, caption=caption_backup, parse_mode="HTML")
                        elif media_type == "photo": await callback.bot.send_photo(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, photo=file_id, caption=caption_backup, parse_mode="HTML")
                        elif media_type == "video": await callback.bot.send_video(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, video=file_id, caption=caption_backup, parse_mode="HTML")
                        elif media_type == "audio": await callback.bot.send_audio(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, audio=file_id, caption=caption_backup, parse_mode="HTML")
                        elif media_type == "voice": await callback.bot.send_voice(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, voice=file_id, caption=caption_backup, parse_mode="HTML")
                    except Exception as e_bkp: print(f"Gagal backup bulk: {e_bkp}")

            except Exception as e_kirim: 
                err_msg = str(e_kirim).lower()
                if "thread not found" in err_msg or "topic not found" in err_msg:
                    await db_exec(lambda: supabase.table("topics").delete().eq("message_thread_id", int(thread_id_str)).eq("group_id", group_id_str).execute())
                    return await callback.message.edit_text("❌ <b>GAGAL: TOPIK SUDAH TIDAK ADA!</b>\n\nTopik tujuan ternyata sudah dihapus manual. Silakan ulangi prosesnya.", parse_mode="HTML")
                elif "chat not found" in err_msg or "bot was kicked" in err_msg or "forbidden" in err_msg:
                    await db_exec(lambda: supabase.table("groups").delete().eq("group_id", group_id_str).execute())
                    await db_exec(lambda: supabase.table("topics").delete().eq("group_id", group_id_str).execute())
                    return await callback.message.edit_text("❌ <b>GAGAL: GRUP SUDAH TIDAK ADA/AKSES DITOLAK!</b>", parse_mode="HTML")
                print(f"Error ngirim massal (skip 1 file): {e_kirim}")

        if berhasil_fids:
            for bf in berhasil_fids:
                await db_exec(lambda: supabase.table("upload_queue").delete().eq("file_unique_id", bf).eq("user_id", user_id).execute())
        
        user_res = await db_exec(lambda: supabase.table("users").select("role").eq("user_id", user_id).execute())
        is_superadmin = user_res.data and user_res.data[0].get('role') == 'superadmin'
        backup_msg = " & dibackup!" if is_superadmin and backup_group_id_str and str(group_id_str) != backup_group_id_str else ""
        
        if sukses > 0:
            await callback.message.edit_text(f"🎉 <b>PROSES MASSAL SELESAI!</b>\n\n✅ <b>{sukses} file</b> berhasil dikirim ke <b>{g_name}</b> (Topik: {t_name}){backup_msg}", parse_mode="HTML")
        else:
            await callback.message.edit_text(f"❌ <b>SEMUA FILE GAGAL DIKIRIM!</b>\n\n<i>Tenang, file lu masih aman di antrean. Cek koneksi atau izin bot lu.</i>", parse_mode="HTML")
    except Exception as e: 
        print(f"Error Bulk: {e}")
        await callback.answer(f"Gagal memproses bulk.", show_alert=True)

@dp.callback_query(F.data.startswith("topik_"))
async def pilih_topik(callback: CallbackQuery):
    thread_id_str, fid = callback.data[6:].split("_", 1)
    msg_thread_id = int(thread_id_str) if int(thread_id_str) != 0 else None
    user_id = callback.from_user.id
    try:
        res = await db_exec(lambda: supabase.table("upload_queue").select("*").eq("file_unique_id", fid).eq("user_id", user_id).execute())
        if not res.data: return await callback.message.edit_text("❌ Gagal. Antrean tidak ditemukan.")
        antrean = res.data[0]
        
        safe_display_name = html.escape(antrean['display_name'])
        
        # --- MENERAPKAN FORMAT WIZARD CAPTION ---
        cache_data = wizard_cache.get(fid, {'show_name': True, 'custom_cap': ''})
        caption_parts = []
        if cache_data.get('show_name', True):
            caption_parts.append(f"📁 <b>{safe_display_name}</b>")
        if cache_data.get('custom_cap'):
            caption_parts.append(f"📝 {html.escape(cache_data['custom_cap'])}")
        
        caption = "\n".join(caption_parts).strip()
        # ----------------------------------------

        grp_id_int = int(antrean["group_id"])
        g_res = await db_exec(lambda: supabase.table("groups").select("group_name").eq("group_id", grp_id_int).execute())
        g_name = g_res.data[0]['group_name'] if g_res.data else "Grup"
        
        t_name = "General / Tidak Terdaftar"
        if msg_thread_id:
            t_res = await db_exec(lambda: supabase.table("topics").select("topic_name").eq("group_id", grp_id_int).eq("message_thread_id", msg_thread_id).execute())
            if t_res.data: t_name = t_res.data[0]['topic_name']

        try:
            sent_msg = None
            if antrean['media_type'] == "document": sent_msg = await callback.bot.send_document(chat_id=antrean["group_id"], message_thread_id=msg_thread_id, document=antrean["file_id"], caption=caption, parse_mode="HTML")
            elif antrean['media_type'] == "photo": sent_msg = await callback.bot.send_photo(chat_id=antrean["group_id"], message_thread_id=msg_thread_id, photo=antrean["file_id"], caption=caption, parse_mode="HTML")
            elif antrean['media_type'] == "video": sent_msg = await callback.bot.send_video(chat_id=antrean["group_id"], message_thread_id=msg_thread_id, video=antrean["file_id"], caption=caption, parse_mode="HTML")
            elif antrean['media_type'] == "audio": sent_msg = await callback.bot.send_audio(chat_id=antrean["group_id"], message_thread_id=msg_thread_id, audio=antrean["file_id"], caption=caption, parse_mode="HTML")
            elif antrean['media_type'] == "voice": sent_msg = await callback.bot.send_voice(chat_id=antrean["group_id"], message_thread_id=msg_thread_id, voice=antrean["file_id"], caption=caption, parse_mode="HTML")
            
            msg_id = sent_msg.message_id if sent_msg else None
            await db_exec(lambda: supabase.table("files").upsert({"file_unique_id": fid, "file_id": antrean["file_id"], "display_name": antrean["display_name"], "media_type": antrean["media_type"], "group_id": antrean["group_id"], "message_thread_id": int(thread_id_str), "message_id": msg_id}).execute())
            await db_exec(lambda: supabase.table("upload_queue").delete().eq("file_unique_id", fid).eq("user_id", user_id).execute())

            bg_res = await db_exec(lambda: supabase.table("bot_settings").select("setting_value").eq("setting_key", "backup_group_id").execute())
            backup_group_id_str = bg_res.data[0]['setting_value'] if bg_res.data else None

            if backup_group_id_str and str(antrean["group_id"]) != backup_group_id_str:
                try:
                    map_res = await db_exec(lambda: supabase.table("backup_mapping").select("backup_thread_id").eq("source_group_id", grp_id_int).execute())
                    backup_thread_id = None
                    if map_res.data: backup_thread_id = map_res.data[0]['backup_thread_id']
                    else:
                        new_topic = await callback.bot.create_forum_topic(chat_id=backup_group_id_str, name=g_name)
                        backup_thread_id = new_topic.message_thread_id
                        await db_exec(lambda: supabase.table("backup_mapping").insert({"source_group_id": grp_id_int, "backup_thread_id": backup_thread_id}).execute())

                    if backup_thread_id:
                        sender_name = f"@{callback.from_user.username}" if callback.from_user.username else callback.from_user.full_name
                        
                        # --- CAPTIO BACKUP DISESUAIKAN JUGA ---
                        caption_backup_parts = []
                        if cache_data.get('show_name', True):
                            caption_backup_parts.append(f"📁 <b>{safe_display_name}</b>")
                        if cache_data.get('custom_cap'):
                            caption_backup_parts.append(f"📝 {html.escape(cache_data['custom_cap'])}")
                        
                        caption_backup_parts.append(f"🏢 Asal: {g_name} (Topik: {t_name})\n👤 Pengirim: {sender_name} (Via Bot Japri)")
                        caption_backup = "\n".join(caption_backup_parts).strip()
                        # --------------------------------------

                        if antrean['media_type'] == "document": await callback.bot.send_document(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, document=antrean["file_id"], caption=caption_backup, parse_mode="HTML")
                        elif antrean['media_type'] == "photo": await callback.bot.send_photo(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, photo=antrean["file_id"], caption=caption_backup, parse_mode="HTML")
                        elif antrean['media_type'] == "video": await callback.bot.send_video(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, video=antrean["file_id"], caption=caption_backup, parse_mode="HTML")
                        elif antrean['media_type'] == "audio": await callback.bot.send_audio(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, audio=antrean["file_id"], caption=caption_backup, parse_mode="HTML")
                        elif antrean['media_type'] == "voice": await callback.bot.send_voice(chat_id=backup_group_id_str, message_thread_id=backup_thread_id, voice=antrean["file_id"], caption=caption_backup, parse_mode="HTML")
                except Exception as eb: print(f"Gagal backup satuan: {eb}")

        except Exception as e_kirim:
            err_msg = str(e_kirim).lower()
            if "thread not found" in err_msg or "topic not found" in err_msg:
                await db_exec(lambda: supabase.table("topics").delete().eq("message_thread_id", int(thread_id_str)).eq("group_id", antrean["group_id"]).execute())
                return await callback.message.edit_text("❌ <b>GAGAL: TOPIK SUDAH TIDAK ADA!</b>\n\nTopik tujuan ternyata sudah dihapus manual. Silakan pilih topik lain.", parse_mode="HTML")
            elif "chat not found" in err_msg or "bot was kicked" in err_msg or "forbidden" in err_msg:
                await db_exec(lambda: supabase.table("groups").delete().eq("group_id", antrean["group_id"]).execute())
                await db_exec(lambda: supabase.table("topics").delete().eq("group_id", antrean["group_id"]).execute())
                return await callback.message.edit_text("❌ <b>GAGAL: GRUP SUDAH TIDAK ADA/AKSES DITOLAK!</b>", parse_mode="HTML")
            
            return await callback.message.edit_text(f"❌ <b>Gagal mengirim file!</b>\nAlasan: <code>{e_kirim}</code>\n\n<i>Tenang, file lu masih aman di antrean. Silakan coba lagi.</i>", parse_mode="HTML")

        user_res = await db_exec(lambda: supabase.table("users").select("role").eq("user_id", user_id).execute())
        is_superadmin = user_res.data and user_res.data[0].get('role') == 'superadmin'
        backup_msg = " & dibackup!" if is_superadmin and backup_group_id_str and str(antrean["group_id"]) != backup_group_id_str else ""

        # Bersihkan memori cache sesudah berhasil
        if fid in wizard_cache: del wizard_cache[fid]

        try: await callback.message.delete()
        except: pass
        await callback.bot.send_message(chat_id=user_id, text=f"🎉 <b>SUKSES!</b>\n\nFile <b>{safe_display_name}</b> berhasil dikirim ke <b>{g_name}</b> (Topik: {t_name}){backup_msg}", parse_mode="HTML")
        
        teks, markup = await get_queue_ui(callback.from_user.id)
        if markup: await callback.bot.send_message(chat_id=user_id, text=teks, reply_markup=markup, parse_mode="HTML")
    except Exception as e: 
        print(f"Error single: {e}")
        await callback.bot.send_message(chat_id=user_id, text=f"❌ Gagal memproses file. Terjadi kesalahan internal: {e}", parse_mode="HTML")

# ==========================================
# 6. GARBAGE COLLECTOR TUKANG SAPU
# ==========================================
async def queue_garbage_collector():
    while True:
        try:
            gc_res = await db_exec(lambda: supabase.table("bot_settings").select("setting_value").eq("setting_key", "gc_duration_hours").execute())
            gc_hours = int(gc_res.data[0]['setting_value']) if gc_res.data else 48

            time_threshold = (datetime.now(timezone.utc) - timedelta(hours=gc_hours)).isoformat()
            
            res = await db_exec(lambda: supabase.table("upload_queue").delete().lt("created_at", time_threshold).execute())
            if res.data:
                print(f"🧹 GARBAGE COLLECTOR: Berhasil nyapu {len(res.data)} file nyangkut yang usianya lebih dari {gc_hours} jam!")
        except Exception as e:
            print(f"GC Error: {e}")
        
        await asyncio.sleep(3600) 

# ==========================================
# 8. DUMMY WEB SERVER (ANTI-SLEEP RENDER)
# ==========================================
async def handle_web(request):
    return web.Response(text="VaultAssist Bot is Alive and Kicking! 🚀")

async def run_web_server():
    app = web.Application()
    app.router.add_get("/", handle_web)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"🌍 Dummy Web Server nyala di port {port}...")

# ==========================================
# 9. FUNGSI UTAMA (MAIN LOOP)
# ==========================================
async def main():
    bot = Bot(token=TOKEN)
    
    cmd_private = [
        BotCommand(command="start", description="Menyalakan VaultAssist / Refresh"),
        BotCommand(command="help", description="Panduan & Bantuan Penggunaan"),
        BotCommand(command="queue", description="Lihat antrean file"),
        BotCommand(command="files", description="Lihat daftar arsip & grup"),
        BotCommand(command="search", description="Cari file arsip"),
        BotCommand(command="list_topics", description="Lihat struktur folder")
    ]
    await bot.set_my_commands(cmd_private, scope=BotCommandScopeAllPrivateChats())
    
    cmd_group = [
        BotCommand(command="register_topic", description="[Admin] Daftarkan Topik ke DB"),
        BotCommand(command="edit_topic", description="[Admin] Ubah nama Topik"),
        BotCommand(command="delete_topic", description="[Admin] Hapus Topik"),
        BotCommand(command="group_settings", description="[Admin] Atur privasi akses grup")
    ]
    await bot.set_my_commands(cmd_group, scope=BotCommandScopeAllGroupChats())
    
    # Menjalankan Background Tasks
    asyncio.create_task(queue_garbage_collector())
    asyncio.create_task(run_web_server()) # Web Server nyala bareng bot
    
    print("Mengecek sistem... VaultAssist siap beroperasi! 🟢")
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try: asyncio.run(main())
    except KeyboardInterrupt: print("Bot dimatikan.")