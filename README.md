# 📦 RoomArchive Bot (Archie)

RoomArchive (Archie) adalah Telegram Bot tingkat *Enterprise* yang dirancang untuk memanajemen, mengarsipkan, dan mem-backup file secara terstruktur menggunakan fitur **Telegram Forum/Topics**. Bot ini dilengkapi dengan sistem keamanan *Role-Based Access Control* (RBAC), *Disaster Recovery* (Auto-Backup), dan *Garbage Collector* otomatis.

Dibuat menggunakan **Python (Aiogram)** dan **Supabase** sebagai *Database as a Service* (DBaaS).

## ✨ Fitur Utama

- **🔐 Role-Based Access Control (RBAC):** Pemisahan hak akses antara User Biasa, Admin Grup, dan Super Admin (Dewa). Privasi arsip grup sangat terjaga.
- **📂 Struktur Folder Dinamis:** Terintegrasi langsung dengan fitur *Topics* di grup Telegram. Buat topik di grup, otomatis terdaftar sebagai folder di database.
- **🛡️ Disaster Recovery (Brankas Backup):** Setiap file yang dikirim ke grup (CCTV Mode) maupun via antrean Japri akan otomatis digandakan (di-*copy*) ke Grup Brankas Utama secara diam-diam.
- **🛒 Sistem Antrean (Queue):** Upload banyak file (Bulk/Massal) via obrolan pribadi (Japri), beri nama, dan pilih grup/topik tujuan tanpa menyepam grup utama.
- **🧹 Auto Garbage Collector:** Sistem otomatis pembersih antrean (bisa diatur rentang waktunya oleh Super Admin) untuk file yang terbengkalai.
- **🚨 Security Blacklist:** Otomatis memblokir file berbahaya (seperti `.exe`, `.apk`, `.bat`) dan mengamankannya ke Brankas untuk keperluan forensik.
- **🌍 Multi-Bahasa:** Mendukung antarmuka Bahasa Indonesia dan Bahasa Inggris.

## 🛠️ Prasyarat (Prerequisites)

Sebelum menjalankan bot ini, pastikan Anda telah menginstal/menyiapkan:
1. Python 3.9 atau lebih baru.
2. Token Telegram Bot (Dapatkan dari [@BotFather](https://t.me/BotFather)).
3. Proyek [Supabase](https://supabase.com/) yang sudah berjalan.

## ⚙️ Konfigurasi & Instalasi

1. **Clone repositori ini:**
   ```bash
   git clone [https://github.com/username-anda/roomarchive-bot.md](https://github.com/username-anda/roomarchive-bot.md)
   cd roomarchive-bot