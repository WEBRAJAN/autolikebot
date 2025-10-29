import json
import time
import threading
import requests
import io
import os
import re
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed # Explicit import

from github import Github, BadCredentialsException, UnknownObjectException
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- [START] State constants (Ensure these are present) ---
STATE_AWAITING = "awaiting"
STATE_GITHUB_TOKEN = "github_token"
STATE_GITHUB_REPO = "github_repo"
STATE_GITHUB_FILE = "github_file"
STATE_JWT_API = "jwt_api"
STATE_LIKE_API = "like_api"
STATE_GUEST_ACCOUNTS = "guest_accounts"
STATE_GITHUB_TOKEN_SETUP = "github_token_setup"
STATE_TARGET_UID_ADD = "target_uid_add"
STATE_TARGET_UID_REMOVE = "target_uid_remove"
STATE_JWT_MANUAL_FILE = "jwt_manual_file"
STATE_GITHUB_EDIT_CONTENT = "github_edit_content"
# --- [END] State constants ---

# *** NEW: Define Persistent Disk Path ***
# Render disk /data par mount hoga
PERSISTENT_DATA_PATH = "/data"
SETTINGS_FILE = os.path.join(PERSISTENT_DATA_PATH, "settings.json")


class LikerManager:
    def __init__(self, bot):
        self.bot = bot
        self.settings = {}  # chat_id: {config}
        self.threads = {}   # chat_id: {'thread': Thread, 'stop_event': Event}
        self.states = {}    # chat_id: {'state': STATE_..., 'data': {...}}
        self.temp_github_sessions = {} # chat_id: Github(token)

        # *** NEW: Load settings on startup ***
        self.load_all_settings()

    # *** NEW: Load settings from file ***
    def load_all_settings(self):
        # Ensure the /data directory exists (for local testing)
        os.makedirs(PERSISTENT_DATA_PATH, exist_ok=True) 
        
        try:
            with open(SETTINGS_FILE, 'r') as f:
                self.settings = json.load(f)
                # Convert string keys back to int
                self.settings = {int(k): v for k, v in self.settings.items()}
                print(f"Loaded settings for {len(self.settings)} users from {SETTINGS_FILE}")
        except FileNotFoundError:
            print(f"No settings file found at {SETTINGS_FILE}. Starting fresh.")
            self.settings = {}
        except Exception as e:
            print(f"Error loading settings: {e}. Starting fresh.")
            self.settings = {}

    # *** NEW: Save settings to file ***
    def save_all_settings(self):
        try:
            # Save settings (keys must be strings for JSON)
            with open(SETTINGS_FILE, 'w') as f:
                json.dump(self.settings, f, indent=4)
            # print(f"Saved settings to {SETTINGS_FILE}")
        except Exception as e:
            print(f"CRITICAL: Failed to save settings! {e}")

    def _clear_state(self, chat_id):
        """User ke current state ko clear karta hai"""
        self.states.pop(chat_id, None)

    def _get_config(self, chat_id):
        """User ka config data fetch karta hai"""
        # Setdefault ensures the chat_id key exists
        return self.settings.setdefault(chat_id, {})

    def _check_config_complete(self, chat_id):
        """Check karta hai ki Liker setup poora hua ya nahi"""
        config = self._get_config(chat_id)
        required_keys = ['jwt_api', 'like_api', 'guest_accounts', 'github_token', 'github_repo', 'github_file_path', 'target_uids']

        # Check karein ki key maujood hai aur empty nahi hai
        for key in required_keys:
            if not config.get(key):
                return False
        return True


    # =====================================
    # GENERAL HANDLERS
    # =====================================

    def handle_start(self, message):
        chat_id = message.chat.id
        self._clear_state(chat_id)

        text = "👋 <b>Namaste! Main ek multi-functional bot hoon.</b>\n\nAap neeche diye gaye commands ka istemal kar sakte hain:"

        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("🧾 JSON Converter", callback_data="menu:json"),
            InlineKeyboardButton("✍️ GitHub Editor", callback_data="menu:github"),
            InlineKeyboardButton("⚙️ Liker Setup", callback_data="menu:setup_liker"),
            InlineKeyboardButton("▶️ Start Liker", callback_data="menu:start_liker"),
            InlineKeyboardButton("⏹ Stop Liker", callback_data="menu:stop_liker"),
            InlineKeyboardButton("🧪 Run Liker Now", callback_data="menu:run_now"),
            InlineKeyboardButton("🔑 Manual JWT", callback_data="menu:jwt"),
            InlineKeyboardButton("❌ Cancel", callback_data="menu:cancel")
        )
        self.bot.reply_to(message, text, reply_markup=markup, parse_mode="HTML")

    def handle_cancel(self, message):
        chat_id = message.chat.id
        self._clear_state(chat_id)
        self.temp_github_sessions.pop(chat_id, None)
        self.bot.reply_to(message, "✅ Sabhi operations cancel kar diye gaye hain.", reply_markup=telebot.types.ReplyKeyboardRemove())

    def handle_callback(self, call):
        """Sabhi inline button clicks ko route karta hai"""
        chat_id = call.message.chat.id
        data = call.data

        try:
            self.bot.answer_callback_query(call.id)
        except Exception:
            pass

        # Main Menu
        if data.startswith("menu:"):
            command = data.split(":")[1]
            action = getattr(self, f'handle_{command}_command', None) if command not in ['start_liker', 'stop_liker', 'run_now', 'setup_liker', 'cancel'] else None
            if command == 'start_liker': action = self.start_liker_task
            elif command == 'stop_liker': action = self.stop_liker_task
            elif command == 'run_now': action = self.run_liker_now
            elif command == 'setup_liker': action = self.show_liker_setup_menu
            elif command == 'cancel': action = self.handle_cancel

            if action: action(call.message)
            else: print(f"Unknown menu command: {command}")

        # Liker Setup Menu
        elif data.startswith("setup:"):
            self.handle_liker_setup_callback(call)

        # GitHub Browsing (/github feature)
        elif data.startswith("gh_session:"):
            self.handle_github_session_callback(call)

        # GitHub Browsing (/setup_liker feature)
        elif data.startswith("gh_setup:"):
            self.handle_github_setup_callback(call)

        elif data == "noop":
            # Kuch nahi karna (e.g., folder icon button)
            pass

    # =====================================
    # FEATURE 1: JSON CONVERTER (/json)
    # =====================================

    def handle_json_command(self, message):
        chat_id = message.chat.id
        msg = self.bot.send_message(chat_id, "🧾 <b>JSON Converter</b>\n\nPlease send me *any text or file* containing account details...", parse_mode="HTML")
        self.bot.register_next_step_handler(msg, self.process_json_conversion)

    def process_json_conversion(self, message):
        # Yeh function ab utils/json_converter.py mein define hona chahiye
        # For demonstration, assuming it's imported correctly
        try:
            # from utils.json_converter import handle_json_conversion # --- BEHTAR TAREKA
            handle_json_conversion(message, self.bot) # --- ABHI KE LIYE (neeche define hai)
        except ImportError:
            self.bot.send_message(message.chat.id, "❌ Error: JSON Converter utility file not found.")
        except Exception as e:
            self.bot.send_message(message.chat.id, f"❌ JSON Conversion Error: {e}")
            
        self._clear_state(message.chat.id)


    # =====================================
    # FEATURE 2: GITHUB FILE EDITOR (/github)
    # =====================================

    def handle_github_command(self, message):
        chat_id = message.chat.id
        msg = self.bot.send_message(chat_id, "✍️ <b>GitHub File Editor</b>\n\nPlease send your GitHub Personal Access Token...", parse_mode="HTML")
        self.states[chat_id] = {'state': STATE_GITHUB_TOKEN}
        self.bot.register_next_step_handler(msg, self.process_github_session_token)

    def process_github_session_token(self, message):
        chat_id = message.chat.id
        token = message.text
        if not token:
            self.bot.send_message(chat_id, "Token not received. Use /cancel or send token.")
            self.bot.register_next_step_handler(message, self.process_github_session_token)
            return
        try:
            self.bot.send_message(chat_id, "Verifying token...⏳")
            g = Github(token)
            user = g.get_user()
            user.login # Test token
            self.temp_github_sessions[chat_id] = g
            self.bot.send_message(chat_id, f"✅ Token valid! Logged in as <b>{user.login}</b>.", parse_mode="HTML")
            self.show_github_browser(chat_id, g, "gh_session:repo:", "Your Repositories:")
        except BadCredentialsException:
            msg = self.bot.send_message(chat_id, "❌ Invalid GitHub token. Please try again.")
            self.bot.register_next_step_handler(msg, self.process_github_session_token)
        except Exception as e:
            self.bot.send_message(chat_id, f"❌ An error occurred: {e}")
            self._clear_state(chat_id)

    def show_github_browser(self, chat_id, github_instance, callback_prefix, text, repo_name=None, path=''):
        markup = InlineKeyboardMarkup()
        try:
            if not repo_name:
                repos = github_instance.get_user().get_repos()
                for repo in repos: markup.add(InlineKeyboardButton(f"📁 {repo.name}", callback_data=f"{callback_prefix}{repo.full_name}"))
            else:
                repo = github_instance.get_repo(repo_name)
                contents = repo.get_contents(path)
                if path: markup.add(InlineKeyboardButton("⬅️ Back", callback_data=f"{callback_prefix}{repo_name}:{'/'.join(path.split('/')[:-1])}"))
                else: markup.add(InlineKeyboardButton("⬅️ Back to Repos", callback_data=f"{callback_prefix}"))
                folders = [InlineKeyboardButton(f"📁 {item.name}", callback_data=f"{callback_prefix}{repo_name}:{item.path}") for item in contents if item.type == "dir"]
                files = [InlineKeyboardButton(f"📄 {item.name}", callback_data=f"{callback_prefix.replace(':repo:', ':file:')}{repo_name}:{item.path}") for item in contents if item.type != "dir"]
                markup.add(*folders)
                markup.add(*files)
            self.bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
        except Exception as e:
            self.bot.send_message(chat_id, f"❌ GitHub access error: {e}")
            self._clear_state(chat_id)

    def handle_github_session_callback(self, call):
        chat_id = call.message.chat.id
        data = call.data.split(":", 1)[1] # Remove prefix
        g = self.temp_github_sessions.get(chat_id)
        if not g: self.bot.edit_message_text("Session expired. Please start /github again.", chat_id, call.message.message_id); return

        try: self.bot.delete_message(chat_id, call.message.message_id) # Clean up previous message
        except Exception: pass

        if data.startswith("repo:"):
            repo_path_data = data.split(":", 1)[1]
            if not repo_path_data: self.show_github_browser(chat_id, g, "gh_session:repo:", "Your Repositories:")
            else:
                repo_name, path = repo_path_data.split(":", 1) if ":" in repo_path_data else (repo_path_data, "")
                self.show_github_browser(chat_id, g, "gh_session:repo:", f"Browsing: {repo_name}/{path}", repo_name=repo_name, path=path)
        elif data.startswith("file:"):
            _, repo_name, file_path = data.split(":", 2)
            self.states[chat_id] = {'state': STATE_GITHUB_EDIT_CONTENT, 'data': {'repo_name': repo_name, 'file_path': file_path}}
            msg = self.bot.send_message(chat_id, f"📄 Editing <b>{file_path}</b>.\n\nPlease send the new content as text or upload a .txt file:", parse_mode="HTML")
            self.bot.register_next_step_handler(msg, self.process_github_file_content)

    def process_github_file_content(self, message):
        chat_id = message.chat.id
        state_data = self.states.get(chat_id)
        if not state_data or state_data['state'] != STATE_GITHUB_EDIT_CONTENT: return
        g = self.temp_github_sessions.get(chat_id)
        if not g: self.bot.send_message(chat_id, "Session expired. Please start /github again."); return

        repo_name = state_data['data']['repo_name']
        file_path = state_data['data']['file_path']
        content = ""
        try:
            if message.document:
                file_info = self.bot.get_file(message.document.file_id)
                content = self.bot.download_file(file_info.file_path).decode("utf-8")
            elif message.text: content = message.text
            else: self.bot.send_message(chat_id, "Invalid content. Use /cancel to retry."); return

            self.bot.send_message(chat_id, f"⏳ Updating <b>{file_path}</b>...", parse_mode="HTML")
            repo = g.get_repo(repo_name)
            try:
                file_obj = repo.get_contents(file_path)
                commit = repo.update_file(file_path, "Content update from Telegram Bot", content, file_obj.sha); action = "Updated"
            except UnknownObjectException:
                commit = repo.create_file(file_path, "File created from Telegram Bot", content); action = "Created"
            commit_url = commit['commit'].html_url
            self.bot.send_message(chat_id, f"✅ File {action}!\n<b>Commit:</b> <a href='{commit_url}'>{commit_url.split('/')[-1][:7]}</a>", parse_mode="HTML")
        except Exception as e: self.bot.send_message(chat_id, f"❌ Failed to update file: {e}")
        finally: self._clear_state(chat_id); self.temp_github_sessions.pop(chat_id, None)

    # =====================================
    # FEATURE 3: LIKER SETUP (/setup_liker)
    # =====================================

    def show_liker_setup_menu(self, message):
        chat_id = message.chat.id
        # self._clear_state(chat_id) # Don't clear state here, only on start/cancel
        config = self._get_config(chat_id)
        status = { k: ("✅" if config.get(v) else "❌") for k, v in {
            'jwt': 'jwt_api', 'like': 'like_api', 'guests': 'guest_accounts',
            'gh_token': 'github_token', 'gh_path': 'github_repo', 'targets': 'target_uids'}.items()}
        text = "⚙️ <b>Automatic Liker Setup</b>\n\nConfigure all 6 settings for automation:"
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton(f"{status['jwt']} 1. JWT API", callback_data="setup:jwt_api"),
            InlineKeyboardButton(f"{status['like']} 2. Like API", callback_data="setup:like_api"),
            InlineKeyboardButton(f"{status['guests']} 3. Guest Accounts", callback_data="setup:guest_accounts"),
            InlineKeyboardButton(f"{status['gh_token']} 4. GitHub Token", callback_data="setup:github_token"),
            InlineKeyboardButton(f"{status['gh_path']} 5. GitHub Path", callback_data="setup:github_path"),
            InlineKeyboardButton(f"{status['targets']} 6. Target UIDs", callback_data="setup:target_uids"),
            InlineKeyboardButton("Done (Main Menu)", callback_data="menu:start"))
        try: self.bot.edit_message_text(text, chat_id, message.message_id, reply_markup=markup, parse_mode="HTML")
        except Exception: self.bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")

    def handle_liker_setup_callback(self, call):
        chat_id = call.message.chat.id
        data = call.data.split(":")[1]
        prompts = {
            "jwt_api": ("Step 1/6: JWT API", "Send the API endpoint (URL)...", STATE_JWT_API),
            "like_api": ("Step 2/6: Like API", "Send the API endpoint (URL)...", STATE_LIKE_API),
            "guest_accounts": ("Step 3/6: Guest Accounts", "Upload a .json file...", STATE_GUEST_ACCOUNTS),
            "github_token": ("Step 4/6: GitHub Token", "Send your GitHub token...", STATE_GITHUB_TOKEN_SETUP),
        }
        if data in prompts:
            title, text, state = prompts[data]
            try:
                msg = self.bot.edit_message_text(f"<b>{title}</b>\n\n{text}", chat_id, call.message.message_id, parse_mode="HTML")
                self.states[chat_id] = {'state': state}
                self.bot.register_next_step_handler(msg, self.process_setup_input)
            except Exception as e:
                 print(f"ERROR: Could not edit message or register handler for {state}: {e}")
                 self.bot.send_message(chat_id, f"Error initiating step: {e}. Please try again or /cancel.")

        elif data == "github_path": self._handle_setup_github_path(call) # Use the updated function
        elif data == "target_uids": self.show_target_uid_menu(call.message)
        elif data == "uid_add": self._handle_setup_uid_add(call)
        elif data == "uid_remove": self._handle_setup_uid_remove(call)
        elif data == "back": self.show_liker_setup_menu(call.message)

    def _handle_setup_github_path(self, call):
        chat_id = call.message.chat.id
        config = self._get_config(chat_id)

        if 'github_token' not in config:
            self.bot.answer_callback_query(call.id, "Set Step 4: GitHub Token first.", show_alert=True)
            return

        try:
            token = config['github_token']
            g = Github(token)
            user = g.get_user()
            user.login # Test the token

            try:
                self.bot.delete_message(chat_id, call.message.message_id)
            except Exception as del_err:
                pass

            new_text = f"<b>Step 5/6: GitHub Token File</b>\nUser: {user.login}.\n\nSelect the repository where you want to save the new JWT tokens:"
            self.show_github_browser(chat_id, g, "gh_setup:repo:", new_text, repo_name=None)

        except BadCredentialsException as auth_err:
            self.bot.send_message(chat_id, f"❌ GitHub Token Error: Invalid credentials. Please set a new token in Step 4.")
            self.show_liker_setup_menu(call.message)

        except Exception as e:
            self.bot.send_message(chat_id, f"❌ Error accessing GitHub: {e}. Set a new token in Step 4.")
            self.show_liker_setup_menu(call.message)

    def _handle_setup_uid_add(self, call):
        chat_id = call.message.chat.id
        try: self.bot.delete_message(chat_id, call.message.message_id)
        except Exception: pass
        msg = self.bot.send_message(chat_id, "Send one or more UIDs (space or comma separated):")
        self.states[chat_id] = {'state': STATE_TARGET_UID_ADD}
        self.bot.register_next_step_handler(msg, self.process_setup_input)

    def _handle_setup_uid_remove(self, call):
        chat_id = call.message.chat.id
        try: self.bot.delete_message(chat_id, call.message.message_id)
        except Exception: pass
        msg = self.bot.send_message(chat_id, "Send the UID you want to remove:")
        self.states[chat_id] = {'state': STATE_TARGET_UID_REMOVE}
        self.bot.register_next_step_handler(msg, self.process_setup_input)

    def process_setup_input(self, message):
        """Liker Setup ke liye next_step_handler se input process karta hai"""
        chat_id = message.chat.id
        state_data = self.states.pop(chat_id, None)

        if not state_data:
            return

        state = state_data['state']
        config = self._get_config(chat_id)

        try:
            if message.content_type == 'text' and message.text == '/cancel':
                self.handle_cancel(message)
                return

            if state == STATE_JWT_API:
                if message.content_type != 'text': raise ValueError("Invalid input type, expected text URL")
                url = message.text.strip().rstrip('/')
                config['jwt_api'] = url
                self.bot.send_message(chat_id, "✅ JWT API saved.")

            elif state == STATE_LIKE_API:
                if message.content_type != 'text': raise ValueError("Invalid input type, expected text URL")
                url = message.text.strip().rstrip('/')
                config['like_api'] = url
                self.bot.send_message(chat_id, "✅ Like API saved.")

            elif state == STATE_GUEST_ACCOUNTS:
                if message.content_type != 'document' or not message.document.file_name.endswith('.json'):
                    msg = self.bot.send_message(chat_id, "❌ Invalid file. Please upload a .json file.")
                    self.states[chat_id] = state_data # Restore state
                    self.bot.register_next_step_handler(msg, self.process_setup_input); return
                file_info = self.bot.get_file(message.document.file_id)
                accounts = json.loads(self.bot.download_file(file_info.file_path).decode("utf-8"))
                if not isinstance(accounts, list): raise ValueError("JSON must contain a list")
                config['guest_accounts'] = accounts
                self.bot.send_message(chat_id, f"✅ {len(accounts)} guest accounts saved.")

            elif state == STATE_GITHUB_TOKEN_SETUP:
                if message.content_type != 'text': raise ValueError("Token must be text")
                token = message.text.strip()
                self.bot.send_message(chat_id, "Token verify kiya ja raha hai...⏳")
                g = Github(token); user = g.get_user(); user.login
                config['github_token'] = token
                self.bot.send_message(chat_id, f"✅ Token saved! User: {user.login}")


            elif state in [STATE_TARGET_UID_ADD, STATE_TARGET_UID_REMOVE]:
                if message.content_type != 'text': raise ValueError("UIDs must be text")
                target_list = config.setdefault('target_uids', [])
                if state == STATE_TARGET_UID_ADD:
                    uids = re.findall(r'\d+', message.text)
                    if not uids: self.bot.send_message(chat_id, "No valid UIDs found.")
                    else:
                        count = sum(1 for uid in uids if uid not in target_list and target_list.append(uid) is None)
                        self.bot.send_message(chat_id, f"✅ Added {count} new Target UIDs. Total: {len(target_list)}")
                elif state == STATE_TARGET_UID_REMOVE:
                    uid_to_remove = message.text.strip()
                    if uid_to_remove in target_list:
                        target_list.remove(uid_to_remove)
                        self.bot.send_message(chat_id, f"✅ Removed UID {uid_to_remove}. Total: {len(target_list)}")
                    else: self.bot.send_message(chat_id, f"⚠️ UID {uid_to_remove} not found in list.")
                
                # *** NEW: Save settings after UID change ***
                self.save_all_settings()
                self.show_target_uid_menu(message); return # Go back to UID menu
            
            # *** NEW: Save settings after every successful step ***
            self.save_all_settings()

        except Exception as e:
            print(f"ERROR in process_setup_input (State: {state}): {e}") # Log error to console
            self.bot.send_message(chat_id, f"❌ Error: {e}\nPlease try again or use /cancel.")

        self.show_liker_setup_menu(message)

    def handle_github_setup_callback(self, call):
        chat_id = call.message.chat.id
        data = call.data.split(":", 1)[1] # Remove prefix
        config = self._get_config(chat_id)
        g = Github(config['github_token'])
        try: self.bot.delete_message(chat_id, call.message.message_id)
        except Exception: pass

        if data.startswith("file:"):
            try: _, repo_name, file_path = data.split(":", 2)
            except ValueError: self.bot.send_message(chat_id, "Error selecting file path. Use /cancel to retry."); return
            config.update({'github_repo': repo_name, 'github_file_path': file_path})
            
            # *** NEW: Save settings ***
            self.save_all_settings()
            
            new_text = (f"✅ <b>Token File Path Saved!</b>\n"
                        f"Repo: <code>{repo_name}</code>\n"
                        f"File: <code>{file_path}</code>\n\n"
                        f"Bot will update this file every 24 hours.")
            self.bot.send_message(chat_id, new_text, parse_mode="HTML")
            self.show_liker_setup_menu(call.message) # Back to setup menu
        elif data.startswith("repo:"):
            repo_path_data = data.split(":", 1)[1]
            if not repo_path_data: # Back to repo list
                new_text = "<b>Step 5/6: GitHub Token File</b>\n\nSelect repository:"
                self.show_github_browser(chat_id, g, "gh_setup:repo:", new_text, repo_name=None)
            else:
                repo_name, path = repo_path_data.split(":", 1) if ":" in repo_path_data else (repo_path_data, "")
                new_text = (f"<b>Step 5/6: GitHub Token File</b>\n\n"
                            f"Select the file to update with JWT tokens (e.g., <code>tokens.json</code>).\n\n" # Updated file extension hint
                            f"Browsing: <code>{repo_name}/{path}</code>")
                self.show_github_browser(chat_id, g, "gh_setup:repo:", new_text, repo_name=repo_name, path=path)

    def show_target_uid_menu(self, message):
        chat_id = message.chat.id
        config = self._get_config(chat_id)
        uids = config.get('target_uids', [])
        text = f"<b>Step 6/6: Target UIDs</b>\n\nCurrent target UIDs: {len(uids)}\n"
        if uids: text += f"<code>{', '.join(uids[:10])}</code>" + ("..." if len(uids) > 10 else "")
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(InlineKeyboardButton("➕ Add UID", callback_data="setup:uid_add"),
                   InlineKeyboardButton("➖ Remove UID", callback_data="setup:uid_remove"),
                   InlineKeyboardButton("⬅️ Back to Setup", callback_data="setup:back"))
        try: self.bot.edit_message_text(text, chat_id, message.message_id, reply_markup=markup, parse_mode="HTML")
        except Exception: self.bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")


    # =====================================
    # FEATURE 4: LIKER CONTROLS & TASK
    # =====================================
    
    def start_liker_task(self, message):
        chat_id = message.chat.id
        if not self._check_config_complete(chat_id): self.bot.send_message(chat_id, "❌ All 6 Liker settings must be completed first."); return
        if chat_id in self.threads: self.bot.send_message(chat_id, "⚠️ Automatic Liker is already running."); return
        stop_event = threading.Event()
        thread = threading.Thread(target=self._task_loop, args=(chat_id, stop_event), daemon=True)
        self.threads[chat_id] = {'thread': thread, 'stop_event': stop_event}
        thread.start()
        self.bot.send_message(chat_id, "▶️ <b>Automatic Liker Started.</b>", parse_mode="HTML")

    def stop_liker_task(self, message):
        chat_id = message.chat.id
        if chat_id not in self.threads: self.bot.send_message(chat_id, "⏹ Automatic Liker is not running."); return
        self.bot.send_message(chat_id, "⏹ Stopping Automatic Liker...")
        self.threads[chat_id]['stop_event'].set()
        self.threads.pop(chat_id, None)

    def run_liker_now(self, message):
        chat_id = message.chat.id
        if not self._check_config_complete(chat_id): self.bot.send_message(chat_id, "❌ All 6 Liker settings must be completed first."); return
        self.bot.send_message(chat_id, "🧪 Running the Liker task once now...")
        threading.Thread(target=self._run_task_logic, args=(chat_id, True), daemon=True).start()

    # --- JWT Fetching (Using Super-Bot Style) ---
    def _fetch_single_jwt_token(self, account, api_url):
        uid, password = account.get("uid"), account.get("password")
        if not uid or not password: return None, "Missing uid or password"
        full_url = f"{api_url}?uid={uid}&password={password}"
        try:
            response = requests.get(full_url, timeout=60) # Keep 60s timeout
            if response.status_code == 200:
                token_data = response.json()
                token_str = None
                if isinstance(token_data, dict) and 'token' in token_data: token_str = token_data['token']
                elif isinstance(token_data, list) and token_data and isinstance(token_data[0], dict) and 'token' in token_data[0]: token_str = token_data[0].get('token')
                if token_str: return {"token": token_str}, None
                else: return None, f"Token key/format invalid: {str(token_data)[:100]}"
            else: return None, f"API failed (Status: {response.status_code})"
        except requests.exceptions.Timeout: return None, "Timeout (60s)"
        except requests.exceptions.ConnectionError as e:
            if isinstance(e.args[0], requests.packages.urllib3.exceptions.ProtocolError) and isinstance(e.args[0].args[1], ConnectionResetError): return None, "Connection Reset by Server"
            else: return None, f"Connection Error: {e}"
        except Exception as e: return None, f"General Error: {e}"


    def _fetch_jwt_concurrently_superbot_style(self, chat_id, accounts, api_url):
        tokens_list = []
        success_count, fail_count, total_to_process = 0, 0, 0
        failed_reasons = {}
        total_potential = len(accounts)
        try: # Wrap send_message in try-except
            progress_msg = self.bot.send_message(chat_id, f"⏳ Starting JWT generation for {total_potential} accounts...")
        except Exception as e:
            print(f"ERROR sending initial progress message: {e}")
            progress_msg = None # Indicate message failed

        # *** CHANGE: Set max_workers to 4 ***
        with ThreadPoolExecutor(max_workers=4) as executor: # Changed from 10 to 4
            futures = {}
            for acc in accounts:
                if acc.get("uid") and acc.get("password"):
                    total_to_process += 1
                    future = executor.submit(self._fetch_single_jwt_token, acc, api_url)
                    futures[future] = acc.get("uid", "N/A")

            processed_count = 0
            for future in as_completed(futures):
                processed_count += 1
                uid = futures[future]
                result_dict, error_reason = future.result()
                if result_dict:
                    tokens_list.append(result_dict)
                    success_count += 1
                else:
                    fail_count += 1
                    failed_reasons[error_reason] = failed_reasons.get(error_reason, 0) + 1
                    # print(f"Debug: JWT failed for UID {uid}, Reason: {error_reason}") # Uncomment for server logs

                if progress_msg and (processed_count % 5 == 0 or processed_count == total_to_process): # Update every 5
                    try:
                        self.bot.edit_message_text(f"⏳ Processed {processed_count}/{total_to_process} JWT requests...", chat_id, progress_msg.message_id)
                    except telebot.apihelper.ApiTelegramException: pass # Ignore edit errors

        # Send final completion message only if initial message was sent
        if progress_msg:
            try:
                self.bot.edit_message_text(f"✅ JWT Processing Complete for {total_to_process} accounts.", chat_id, progress_msg.message_id)
            except telebot.apihelper.ApiTelegramException:
                 self.bot.send_message(chat_id, f"✅ JWT Processing Complete for {total_to_process} accounts.")

        if fail_count > 0:
            fail_summary = "⚠️ JWT Generation Failures:\n" + "\n".join([f"- {reason}: {count} times" for reason, count in failed_reasons.items()])
            try: self.bot.send_message(chat_id, fail_summary)
            except Exception as e: print(f"Error sending failure summary: {e}")

        return tokens_list, success_count, fail_count, total_to_process

    # Liker Core Logic (_task_loop, _run_task_logic)
    def _task_loop(self, chat_id, stop_event):
        while not stop_event.is_set():
            self.bot.send_message(chat_id, "ℹ️ 24-hour cycle starting...")
            self._run_task_logic(chat_id, is_manual_run=False)
            
            self.bot.send_message(chat_id, "ℹ️ Cycle complete. Waiting 24 hours...")
            wait_seconds = 24 * 60 * 60
            
            # Sleep in 60-second intervals to check for stop_event
            for _ in range(wait_seconds // 60):
                if stop_event.is_set(): break
                time.sleep(60)
                
        self.bot.send_message(chat_id, "ℹ️ Automatic Liker thread stopped.")

    def _run_task_logic(self, chat_id, is_manual_run=False):
        try:
            config = self._get_config(chat_id)
            if not config: return
            
            # =================================
            # Step 1: Generate JWTs
            # =================================
            self.bot.send_message(chat_id, "<b>Task Step 1/3: Generating JWTs...</b>", parse_mode="HTML")
            guest_accounts = config.get('guest_accounts', [])
            if not guest_accounts: self.bot.send_message(chat_id, "❌ Guest Accounts not set. Task cancelled."); return
            
            token_dicts, jwt_success, jwt_fail, jwt_total = self._fetch_jwt_concurrently_superbot_style(chat_id, guest_accounts, config['jwt_api'])
            jwt_summary = (f"📊 <b>Step 1/3 Result:</b>\nProcessed: {jwt_total}, Generated: {jwt_success}, Failed: {jwt_fail}")
            self.bot.send_message(chat_id, jwt_summary, parse_mode="HTML")
            
            if not token_dicts: self.bot.send_message(chat_id, "❌ No tokens generated. Task cancelled."); return

            # =================================
            # Step 2: Update GitHub
            # =================================
            self.bot.send_message(chat_id, "<b>Task Step 2/3: Updating GitHub...</b>", parse_mode="HTML")
            github_status_message = "Fail"
            try:
                g = Github(config['github_token'])
                repo = g.get_repo(config['github_repo'])
                file_path = config['github_file_path']
                new_content = json.dumps(token_dicts, indent=4)
                try:
                    file_obj = repo.get_contents(file_path)
                    if file_obj.decoded_content.decode('utf-8') != new_content:
                        repo.update_file(file_path, "Auto-update tokens", new_content, file_obj.sha); github_status_message = "Updated"
                    else: github_status_message = "Unchanged"
                except UnknownObjectException:
                    repo.create_file(file_path, "Create token file", new_content); github_status_message = "Created"
                self.bot.send_message(chat_id, f"✅ <b>Step 2/3 Complete:</b> GitHub status: {github_status_message}.")
            except Exception as e: self.bot.send_message(chat_id, f"❌ <b>Step 2/3 Error:</b> GitHub update failed: {e}", parse_mode="HTML"); return

            # =================================
            # *** Wait 1 Minute ***
            # =================================
            self.bot.send_message(chat_id, "ℹ️ Waiting 60 seconds for GitHub to update before sending likes...")
            time.sleep(60)

            # =================================
            # Step 3: Send Likes
            # =================================
            self.bot.send_message(chat_id, "<b>Task Step 3/3: Sending Likes...</b>", parse_mode="HTML")
            like_api_url = config['like_api']
            target_uids = config.get('target_uids', [])
            like_success, like_fail = 0, 0
            
            all_like_responses = [] 

            if not target_uids: self.bot.send_message(chat_id, "⚠️ No Target UIDs set. Skipping likes.")
            else:
                for uid in target_uids:
                    response_text = ""
                    try:
                        params = {'uid': uid, 'server_name': 'ind'}
                        response = requests.get(like_api_url, params=params, timeout=60)
                        response_text = response.text # Store response text
                        
                        # *** Retry Logic ***
                        try:
                            error_data = response.json()
                            if response.status_code != 200 and error_data.get("error") == "Failed to retrieve initial player info.":
                                self.bot.send_message(chat_id, f"⚠️ Error for UID {uid}: 'Failed to retrieve...'. Waiting 2 minutes to retry.")
                                time.sleep(120) # Wait 2 minutes
                                
                                # Retry the request
                                response = requests.get(like_api_url, params=params, timeout=60)
                                response_text = response.text # Store new response text
                        except Exception:
                            pass # Not a JSON error or not the specific error, just proceed
                        
                        # --- End Retry Logic ---

                        if response.status_code == 200: 
                            like_success += 1
                        else: 
                            like_fail += 1
                            
                    except Exception as e: 
                        like_fail += 1
                        response_text = f"Request Error: {e}"
                    
                    all_like_responses.append(f"<b>UID {uid}:</b>\n<pre>{response_text}</pre>")
                    time.sleep(1) # Sleep between requests
            
            # --- Send all Like responses
            if all_like_responses:
                self.bot.send_message(chat_id, "📊 <b>Step 3/3 Results (Exact Responses):</b>", parse_mode="HTML")
                
                # Send responses in chunks to avoid message limits
                current_message = ""
                for resp_line in all_like_responses:
                    if len(current_message) + len(resp_line) > 4000:
                        self.bot.send_message(chat_id, current_message, parse_mode="HTML")
                        current_message = resp_line
                    else:
                        current_message += "\n" + resp_line
                
                if current_message: # Send the last chunk
                    self.bot.send_message(chat_id, current_message, parse_mode="HTML")
            
            # =================================
            # Final Summary
            # =================================
            final_summary = (f"🎉 <b>Task Complete!</b>\n"
                             f"JWTs: {jwt_success}/{jwt_total} ok\n"
                             f"GitHub: {github_status_message}\n"
                             f"Likes: {like_success} success, {like_fail} fail")
            self.bot.send_message(chat_id, final_summary, parse_mode="HTML")

        except Exception as e:
             self.bot.send_message(chat_id, f"❌ <b>Task Failed Unexpectedly!</b>\n<pre>{e}</pre>", parse_mode="HTML")
             if not is_manual_run and chat_id in self.threads: self.threads[chat_id]['stop_event'].set(); self.threads.pop(chat_id, None)


    # =====================================
    # FEATURE 5: MANUAL JWT (/jwt)
    # =====================================
    
    def handle_jwt_command(self, message):
        chat_id = message.chat.id
        config = self._get_config(chat_id)
        if 'jwt_api' not in config: self.bot.send_message(chat_id, "❌ Set JWT API in /setup_liker first."); return
        msg = self.bot.send_message(chat_id, "🔑 Manual JWT Generator\nUpload .json file...")
        self.states[chat_id] = {'state': STATE_JWT_MANUAL_FILE}
        self.bot.register_next_step_handler(msg, self.process_manual_jwt_file)

    def process_manual_jwt_file(self, message):
        chat_id = message.chat.id
        state_data = self.states.pop(chat_id, None)
        if not state_data or state_data['state'] != STATE_JWT_MANUAL_FILE: return
        config = self._get_config(chat_id)
        jwt_api_url = config['jwt_api']
        if message.content_type != 'document' or not message.document.file_name.endswith('.json'):
            msg = self.bot.send_message(chat_id, "❌ Invalid file. Upload .json.")
            self.states[chat_id] = state_data
            self.bot.register_next_step_handler(msg, self.process_manual_jwt_file); return
        try:
            # self.bot.send_message(chat_id, "Processing file... (Fast Mode) ⏳") # Removed, _fetch... sends its own
            file_info = self.bot.get_file(message.document.file_id)
            accounts = json.loads(self.bot.download_file(file_info.file_path).decode("utf-8"))
            if not isinstance(accounts, list): raise ValueError("JSON must be a list")
            
            token_dicts, jwt_success, jwt_fail, jwt_total = self._fetch_jwt_concurrently_superbot_style(chat_id, accounts, jwt_api_url)
            
            jwt_summary = (f"📊 <b>Manual JWT Summary:</b>\nProcessed: {jwt_total}, Generated: {jwt_success}, Failed: {jwt_fail}")
            self.bot.send_message(chat_id, jwt_summary, parse_mode="HTML")
            
            if not token_dicts: return
            
            tokens_for_output = [d['token'] for d in token_dicts]
            token_text = "\n".join(tokens_for_output)
            
            if len(token_text) > 4000:
                filename = f"manual_jwts_{chat_id}.txt"
                with io.open(filename, "w", encoding="utf-8") as f: f.write(token_text)
                with open(filename, "rb") as f: self.bot.send_document(chat_id, f, caption=f"Generated {len(tokens_for_output)} tokens.")
                os.remove(filename)
            elif token_text: self.bot.send_message(chat_id, f"🔑 Generated Tokens:\n<pre>{token_text}</pre>", parse_mode="HTML")
        except Exception as e: self.bot.send_message(chat_id, f"❌ Error processing file: {e}")

# =====================================
# BOT INITIALIZATION
# =====================================

# --- [START] JSON Converter Utility Function ---
# Is file ko 'utils/json_converter.py' mein hona chahiye, 
# lekin error se bachne ke liye main ise yahaan add kar raha hoon.
# NOTE: Behtar hoga agar aap 'utils' folder banakar usmein yeh function daalein.

def _extract_accounts(text_data):
    """Text se UID aur Password extract karta hai."""
    accounts = []
    # Broad pattern to find uid/pass variations
    pattern = re.compile(
        r"(?:uid|jio_uid|username|email)\s*[:=\s]\s*(.+?)\s*\n"
        r"(?:password|pass|pwd)\s*[:=\s]\s*(.+?)",
        re.IGNORECASE | re.MULTILINE
    )
    matches = pattern.findall(text_data)
    for uid, password in matches:
        accounts.append({"uid": uid.strip(), "password": password.strip()})
    return accounts

def handle_json_conversion(message, bot_instance):
    """
    Handles file/text input for JSON conversion.
    This function should be in 'utils/json_converter.py'
    """
    chat_id = message.chat.id
    text_data = ""
    accounts = []
    
    try:
        if message.content_type == 'text':
            text_data = message.text
        elif message.content_type == 'document':
            file_info = bot_instance.get_file(message.document.file_id)
            file_data = bot_instance.download_file(file_info.file_path)
            try:
                text_data = file_data.decode('utf-8')
            except UnicodeDecodeError:
                bot_instance.send_message(chat_id, "⚠️ File encoding is not UTF-8, trying 'latin-1'...")
                text_data = file_data.decode('latin-1', errors='ignore')
            
            # Agar file .json hai
            if message.document.file_name.endswith('.json'):
                try:
                    accounts = json.loads(text_data)
                    if not isinstance(accounts, list):
                         accounts = [] # Agar format sahi nahi hai toh extraction try karein
                except json.JSONDecodeError:
                    pass # Extraction try karein
        else:
            bot_instance.send_message(chat_id, "Invalid input. Please send text or a file.")
            return

        # Agar accounts JSON se load nahi hue, toh extract karein
        if not accounts:
            accounts = _extract_accounts(text_data)
            
        if not accounts:
            bot_instance.send_message(chat_id, "❌ No accounts found. Format must be:\n`uid: ...`\n`password: ...`")
            return

        total_accounts = len(accounts)
        bot_instance.send_message(chat_id, f"✅ Found {total_accounts} accounts. Generating files...")

        # 100 ke chunks mein divide karna
        chunk_size = 100
        for i in range(0, total_accounts, chunk_size):
            chunk = accounts[i:i + chunk_size]
            json_data = json.dumps(chunk, indent=4)
            json_bytes = io.BytesIO(json_data.encode('utf-8'))
            
            file_name = f"accounts_{i // chunk_size}.json" if total_accounts > chunk_size else "accounts.json"
            
            bot_instance.send_document(chat_id, json_bytes, visible_file_name=file_name,
                                     caption=f"Here are {len(chunk)} accounts (Part {(i // chunk_size) + 1}).")
    except Exception as e:
        bot_instance.send_message(chat_id, f"❌ An error occurred during conversion: {e}")

# --- [END] JSON Converter Utility Function ---


# --- Main execution ---
if __name__ == "__main__":
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        # *** NEW: Removed input() for Render compatibility ***
        print("CRITICAL: TELEGRAM_BOT_TOKEN environment variable not set.")
        print("Please set it in your deployment environment.")
        exit(1) # Exit if no token is found

    bot = telebot.TeleBot(TOKEN)
    manager = LikerManager(bot)

    # Register handlers
    @bot.message_handler(commands=['start'])
    def start(message): manager.handle_start(message)

    @bot.message_handler(commands=['cancel'])
    def cancel(message): manager.handle_cancel(message)
    
    @bot.message_handler(commands=['json'])
    def json_cmd(message): manager.handle_json_command(message)

    @bot.message_handler(commands=['github'])
    def github_cmd(message): manager.handle_github_command(message)

    @bot.message_handler(commands=['setup_liker'])
    def setup_cmd(message): manager.show_liker_setup_menu(message)
    
    @bot.message_handler(commands=['start_liker'])
    def start_liker_cmd(message): manager.start_liker_task(message)

    @bot.message_handler(commands=['stop_liker'])
    def stop_liker_cmd(message): manager.stop_liker_task(message)

    @bot.message_handler(commands=['run_now'])
    def run_now_cmd(message): manager.run_liker_now(message)
    
    @bot.message_handler(commands=['jwt'])
    def jwt_cmd(message): manager.handle_jwt_command(message)

    @bot.callback_query_handler(func=lambda call: True)
    def callback_query(call): manager.handle_callback(call)

    # Fallback handler for JSON converter (agar state mein nahi hai)
    @bot.message_handler(content_types=['text', 'document'])
    def fallback_handler(message):
        chat_id = message.chat.id
        if chat_id not in manager.states:
            # Agar koi state active nahi hai, toh JSON conversion try karein
            manager.process_json_conversion(message)
        else:
            # Agar state active hai, toh process_setup_input handle karega
            # (lekin yeh register_next_step_handler se trigger hota hai)
            bot.send_message(chat_id, "Bot is currently waiting for a different input. Use /cancel to reset.")

    print("Bot is running...")
    bot.polling(none_stop=True)