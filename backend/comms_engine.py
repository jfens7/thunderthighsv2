# backend/comms_engine.py
import re
import json
import base64
import datetime
import urllib.request
import urllib.parse
from firebase_admin import firestore
import logging

logger = logging.getLogger(__name__)

class CommsEngineMixin:
    """Handles SMS, Notices, Community Forum, and Donations"""

    def get_notices(self):
        if not self.db: return []
        try:
            res = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('notices').order_by('timestamp', direction=firestore.Query.DESCENDING).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['date_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Recent'; data['timestamp'] = str(ts); res.append(data)
            return res
        except: return []

    def admin_add_notice(self, title, message, notice_type, admin_email="Unknown"):
        if not self.db: return False
        try: self.db.collection('notices').add({'title': title, 'message': message, 'type': notice_type, 'timestamp': firestore.SERVER_TIMESTAMP, 'author': admin_email}); self._log_audit(admin_email, 'ADD_NOTICE', f"Posted notice: {title}", {}); return True
        except: return False

    def admin_delete_notice(self, notice_id, admin_email="Unknown"):
        if not self.db: return False
        try: self.db.collection('notices').document(notice_id).delete(); self._log_audit(admin_email, 'DELETE_NOTICE', f"Deleted notice ID: {notice_id}", {}); return True
        except: return False

    def get_admin_messages(self):
        if not self.db: return []
        try:
            res = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('admin_messages').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(50).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['time_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Just now'; res.append(data)
            return res
        except Exception as e: return []

    def add_admin_message(self, message, admin_email="Unknown"):
        if not self.db: return False
        try:
            self.db.collection('admin_messages').add({'message': message, 'author': admin_email, 'timestamp': firestore.SERVER_TIMESTAMP}); self._log_audit(admin_email, 'ADD_NOTE', "Added an admin note", {})
            return True
        except: return False

    def get_community_feed(self):
        if not self.db: return []
        try:
            posts = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('community_posts').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(50).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['time_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%y %I:%M %p') if ts else 'Just now'; data['timestamp'] = str(ts); posts.append(data)
            return posts
        except: return []

    def create_community_post(self, author_uid, author_name, content, post_type="General", image_url=None, poll_options=None):
        if not self.db: return False
        try:
            payload = {'author_uid': author_uid, 'author_name': author_name, 'content': content, 'type': post_type, 'image_url': image_url, 'upvotes': [], 'comments': [], 'timestamp': firestore.SERVER_TIMESTAMP}
            if poll_options: payload['poll'] = {opt: [] for opt in poll_options}
            self.db.collection('community_posts').add(payload); return True
        except: return False

    def vote_community_poll(self, post_id, option_text, voter_uid):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('community_posts').document(post_id); doc = doc_ref.get()
            if not doc.exists: return False
            poll = doc.to_dict().get('poll', {})
            for opt, voters in poll.items():
                if voter_uid in voters: voters.remove(voter_uid)
            if option_text in poll: poll[option_text].append(voter_uid)
            doc_ref.update({'poll': poll}); return True
        except: return False

    def add_community_comment(self, post_id, author_name, content):
        if not self.db: return False
        try:
            self.db.collection('community_posts').document(post_id).update({'comments': firestore.ArrayUnion([{'author': author_name, 'content': content, 'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()}])}); return True
        except: return False

    def toggle_post_upvote(self, post_id, uid):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('community_posts').document(post_id); doc = doc_ref.get()
            if not doc.exists: return False
            upvotes = doc.to_dict().get('upvotes', [])
            if uid in upvotes: upvotes.remove(uid)
            else: upvotes.append(uid)
            doc_ref.update({'upvotes': upvotes}); return True
        except: return False

    def admin_delete_community_post(self, post_id, admin_email="Unknown"):
        if not self.db: return False
        try: self.db.collection('community_posts').document(post_id).delete(); self._log_audit(admin_email, 'DELETE_POST', f"Deleted community post {post_id}", {}); return True
        except: return False

    def get_contact_lists(self):
        if not hasattr(self, 'sheet_results') or not self.sheet_results: return {"emails": "", "phones": "", "preview": [], "error": "No sheet connection"}
        try:
            ws = self.sheet_results.worksheet("Member info"); raw_data = ws.get_all_values()
            if not raw_data or len(raw_data) < 2: return {"emails": "", "phones": "", "preview": [], "error": "Sheet appears to be empty."}
            headers = [str(h).lower().strip() for h in raw_data[0]]; has_send_col = any('send' in h for h in headers)
            emails = []; phones = []; preview = []
            for row in raw_data[1:]:
                if not any(str(cell).strip() for cell in row): continue
                email_val = ""; phone_val = ""; name_val = "Unknown Player"; allow_sms = not has_send_col 
                for idx, cell_val in enumerate(row):
                    if idx >= len(headers): break
                    col_name = headers[idx]; val_str = str(cell_val).strip()
                    if col_name in ['name', 'player', 'player name', 'full name', 'first name'] and name_val == "Unknown Player": name_val = val_str
                    if 'email' in col_name and not email_val: email_val = val_str
                    if ('phone' in col_name or 'mobile' in col_name or 'number' in col_name) and not phone_val: phone_val = val_str
                    if 'send' in col_name: allow_sms = val_str.lower() in ['yes', 'y', 'true']
                if email_val and '@' in email_val: emails.append(email_val)
                if phone_val and allow_sms:
                    clean_phone = re.sub(r'[^\d\+\s]', '', phone_val)
                    if len(clean_phone) >= 8: 
                        phones.append(clean_phone); main_div = "Unknown Div"; clean_name_key = self._clean_name(name_val)
                        if clean_name_key in self.all_players:
                            div_counts = {}
                            for h in self.all_players[clean_name_key].get('combined', {}).get('history', []): div_counts[h['division']] = div_counts.get(h['division'], 0) + 1
                            if div_counts: main_div = max(div_counts, key=div_counts.get)
                        preview.append({"name": name_val, "phone": clean_phone, "division": main_div})
            return {"emails": ", ".join(list(set(emails))), "phones": ", ".join(list(set(phones))), "preview": preview}
        except Exception as e: return {"emails": "", "phones": "", "preview": [], "error": str(e)}

    def admin_send_sms_broadcast(self, message_body, target_phones=None, admin_email="Unknown"):
        contacts = self.get_contact_lists()
        raw_phones = contacts.get("phones", "")
        if not raw_phones: return {"success": False, "error": "No valid opted-in phone numbers found in the Google Sheet."}
        
        allowed_phone_list = [p.strip() for p in raw_phones.split(",") if p.strip()]
        phone_to_name = {c["phone"]: c["name"] for c in contacts.get("preview", [])}
            
        phone_list = [p for p in target_phones if p in allowed_phone_list] if target_phones is not None else allowed_phone_list
        if not phone_list: return {"success": False, "error": "No valid opted-in phone numbers matched your selection."}
        
        username = "jakobwill7@gmail.com"
        api_key = "76F26417-8DEB-E47E-8056-B86E519B4445"
        clean_body = re.sub(r'[^\x20-\x7E\n\r]+', '', message_body)
        messages = []

        for phone in phone_list:
            player_name = phone_to_name.get(phone, "")
            profile_link = f"gctta-stats.com.au/?player={urllib.parse.quote(player_name)}" if player_name and player_name != "Unknown Player" else "gctta-stats.com.au"

            if "{link}" in clean_body:
                final_message = f"GCTTA: {clean_body.replace('{link}', profile_link)}\n\nReply STOP to opt out"
            else:
                final_message = f"GCTTA Update: {clean_body}\n\nStats: {profile_link}\nReply STOP to opt out"
                
            messages.append({"source": "gctta_admin", "from": "GCTTA-STATS", "body": final_message, "to": phone})
            
        payload = json.dumps({"messages": messages}).encode('utf-8')
        
        try:
            import ssl
            ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
            auth_str = f"{username}:{api_key}"; auth_bytes = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
            
            req = urllib.request.Request("https://rest.clicksend.com/v3/sms/send", data=payload)
            req.add_header("Content-Type", "application/json")
            req.add_header("Authorization", f"Basic {auth_bytes}")
            
            response = urllib.request.urlopen(req, context=ctx)
            res_data = json.loads(response.read().decode('utf-8'))
            
            if res_data.get('http_code') == 200:
                self._log_audit(admin_email, 'SMS_BROADCAST', f"Sent Mass SMS to {len(phone_list)} members", {})
                return {"success": True, "message": f"Successfully sent Custom SMS to {len(phone_list)} members!"}
            else: return {"success": False, "error": f"ClickSend API Error: {res_data}"}
        except Exception as e: return {"success": False, "error": str(e)}

    def get_sms_inbox(self):
        if not self.db: return []
        try:
            res = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('sms_replies').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(50).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp')
                data['time_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Just now'; data['timestamp'] = str(ts); res.append(data)
            return res
        except: return []

    def admin_get_all_donations(self):
        if not self.db: return []
        try:
            res = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('donations').order_by('timestamp', direction=firestore.Query.DESCENDING).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['time_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Unknown'; res.append(data)
            return res
        except: return []

    def record_donation(self, intent_id, name, amount):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('donations').document(intent_id)
            if doc_ref.get().exists: return True 
            doc_ref.set({'name': name if name and name.strip() else 'Anonymous', 'amount': float(amount), 'timestamp': firestore.SERVER_TIMESTAMP, 'month': datetime.datetime.now().strftime("%Y-%m")}); return True
        except: return False

    def get_top_donors(self, limit=5):
        if not self.db: return []
        try:
            grouped_donors = {}; donors = []
            for d in self.db.collection('donations').stream():
                data = d.to_dict(); n = data.get('name', 'Anonymous').strip()
                amt = float(data.get('amount', 0))
                if n.lower() == 'anonymous': donors.append({'name': 'Anonymous', 'amount': amt})
                else: grouped_donors[n] = grouped_donors.get(n, 0) + amt
            for k, v in grouped_donors.items(): donors.append({'name': k, 'amount': v})
            donors.sort(key=lambda x: x['amount'], reverse=True); return donors[:limit]
        except: return []