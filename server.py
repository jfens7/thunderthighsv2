import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

import logging
import datetime
import threading
import stripe
import re
import json
import base64
import hashlib
import struct
from functools import wraps
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, make_response
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from firebase_admin import auth as fb_auth
from google.cloud import firestore

from backend.backend import ThunderData

app = Flask(__name__, static_folder="frontend/static", template_folder="frontend/templates")
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'gctta-super-secret-session-key')
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY', '').strip(' "\'')
app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(days=30)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db = None
db_lock = threading.Lock()
sync_lock = threading.Lock()
is_first_snapshot = True


# ==========================================
# WEBAUTHN / ICLOUD PASSKEY CRYPTO HELPERS
# ==========================================
def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def _b64url_decode(val: str) -> bytes:
    padding_len = (4 - len(val) % 4) % 4
    return base64.urlsafe_b64decode(val + ('=' * padding_len))


def _get_rp_id() -> str:
    host = request.host.split(':')[0].lower()
    return host


def _decode_cbor_item(data: bytes, offset: int = 0):
    """Lightweight RFC 8949 CBOR decoder for WebAuthn attestation objects and COSE keys."""
    initial = data[offset]
    offset += 1
    major = initial >> 5
    addl = initial & 0x1f

    if addl < 24:
        val = addl
    elif addl == 24:
        val = data[offset]
        offset += 1
    elif addl == 25:
        val = struct.unpack(">H", data[offset:offset + 2])[0]
        offset += 2
    elif addl == 26:
        val = struct.unpack(">I", data[offset:offset + 4])[0]
        offset += 4
    elif addl == 27:
        val = struct.unpack(">Q", data[offset:offset + 8])[0]
        offset += 8
    else:
        raise ValueError("Unsupported CBOR additional info")

    if major == 0:  # Unsigned int
        return val, offset
    elif major == 1:  # Negative int
        return -1 - val, offset
    elif major == 2:  # Byte string
        res = data[offset:offset + val]
        return res, offset + val
    elif major == 3:  # Text string
        res = data[offset:offset + val].decode('utf-8', errors='replace')
        return res, offset + val
    elif major == 4:  # Array
        arr = []
        for _ in range(val):
            item, offset = _decode_cbor_item(data, offset)
            arr.append(item)
        return arr, offset
    elif major == 5:  # Map
        mp = {}
        for _ in range(val):
            k, offset = _decode_cbor_item(data, offset)
            v, offset = _decode_cbor_item(data, offset)
            mp[k] = v
        return mp, offset
    else:
        return val, offset


def _extract_cose_from_attestation(attestation_b64: str) -> dict:
    att_bytes = _b64url_decode(attestation_b64)
    att_obj, _ = _decode_cbor_item(att_bytes, 0)
    auth_data: bytes = att_obj['authData']

    # 32 bytes rpIdHash + 1 byte flags + 4 bytes signCount = 37 bytes
    flags = auth_data[32]
    sign_count = struct.unpack(">I", auth_data[33:37])[0]
    has_attested_cred = bool(flags & 0x40)
    if not has_attested_cred:
        raise ValueError("No attested credential data in authData")

    # Skip 16 bytes AAGUID (offset 37..53)
    cred_id_len = struct.unpack(">H", auth_data[53:55])[0]
    cred_id = auth_data[55:55 + cred_id_len]
    cose_key_map, _ = _decode_cbor_item(auth_data, 55 + cred_id_len)

    kty = cose_key_map.get(1)  # 2 = EC2 (Apple ES256), 3 = RSA (RS256)
    alg = cose_key_map.get(3)  # -7 = ES256, -257 = RS256

    if kty == 2:
        x_bytes = cose_key_map.get(-2)
        y_bytes = cose_key_map.get(-3)
        return {
            'kty': 'EC2',
            'alg': alg,
            'x': _b64url_encode(x_bytes),
            'y': _b64url_encode(y_bytes),
            'cred_id': _b64url_encode(cred_id),
            'sign_count': sign_count
        }
    elif kty == 3:
        n_bytes = cose_key_map.get(-1)
        e_bytes = cose_key_map.get(-2)
        return {
            'kty': 'RSA',
            'alg': alg,
            'n': _b64url_encode(n_bytes),
            'e': _b64url_encode(e_bytes),
            'cred_id': _b64url_encode(cred_id),
            'sign_count': sign_count
        }
    else:
        raise ValueError(f"Unsupported key type: {kty}")


def _verify_webauthn_assertion(stored_cred: dict, auth_response: dict, expected_challenge: str, expected_rp_id: str) -> bool:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec, rsa, padding

    resp_block = auth_response.get('response', {})
    client_data_bytes = _b64url_decode(resp_block['clientDataJSON'])
    client_data = json.loads(client_data_bytes.decode('utf-8'))

    if client_data.get('type') != 'webauthn.get':
        raise ValueError("Invalid WebAuthn assertion type")
    if client_data.get('challenge') != expected_challenge:
        raise ValueError("Challenge mismatch")

    auth_data_bytes = _b64url_decode(resp_block['authenticatorData'])
    rp_id_hash = hashlib.sha256(expected_rp_id.encode('utf-8')).digest()
    if auth_data_bytes[:32] != rp_id_hash:
        raise ValueError("RP ID hash mismatch")

    flags = auth_data_bytes[32]
    if not (flags & 0x01):  # User Present bit
        raise ValueError("User presence flag not set")

    signature_bytes = _b64url_decode(resp_block['signature'])
    client_data_hash = hashlib.sha256(client_data_bytes).digest()
    signed_payload = auth_data_bytes + client_data_hash

    if stored_cred.get('kty') == 'EC2':
        x_int = int.from_bytes(_b64url_decode(stored_cred['x']), 'big')
        y_int = int.from_bytes(_b64url_decode(stored_cred['y']), 'big')
        pub_numbers = ec.EllipticCurvePublicNumbers(x_int, y_int, ec.SECP256R1())
        pub_key = pub_numbers.public_key()
        pub_key.verify(signature_bytes, signed_payload, ec.ECDSA(hashes.SHA256()))
        return True
    elif stored_cred.get('kty') == 'RSA':
        n_int = int.from_bytes(_b64url_decode(stored_cred['n']), 'big')
        e_int = int.from_bytes(_b64url_decode(stored_cred['e']), 'big')
        pub_numbers = rsa.RSAPublicNumbers(e_int, n_int)
        pub_key = pub_numbers.public_key()
        pub_key.verify(signature_bytes, signed_payload, padding.PKCS1v15(), hashes.SHA256())
        return True
    return False


# ==========================================
# BACKGROUND SYNC & FRIDAY AUTO-PULL
# ==========================================
def scheduled_refresh(commit_history=False):
    global db
    if db and sync_lock.acquire(blocking=False):
        try:
            logger.info("🔄 Executing background database sync...")
            db.refresh_data(commit_history=commit_history)
            if db.db:
                aest_now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=10)))
                db.db.collection('system_config').document('sync_status').set({
                    'last_sync_date': aest_now.strftime("%Y-%m-%d"),
                    'last_sync_time': aest_now.strftime("%Y-%m-%d %I:%M:%S %p AEST"),
                    'timestamp': firestore.SERVER_TIMESTAMP
                }, merge=True)
            logger.info("✅ Background sync complete!")
        except Exception as e:
            logger.error(f"Background Sync Error: {e}")
        finally:
            sync_lock.release()


def check_friday_auto_sync():
    """
    Guarantees Friday morning Google Sheets pull happens even if APScheduler thread
    was recycled by Gunicorn workers.
    """
    global db
    if not db or not db.db or sync_lock.locked():
        return
    try:
        aest_now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=10)))
        if aest_now.weekday() == 4 and aest_now.hour >= 6:
            today_str = aest_now.strftime("%Y-%m-%d")
            doc = db.db.collection('system_config').document('sync_status').get()
            last_date = doc.to_dict().get('last_sync_date', '') if doc.exists else ''
            if last_date != today_str:
                logger.info("📅 Friday Auto-Sync Check: Triggering weekly Google Sheets pull...")
                threading.Thread(target=scheduled_refresh, kwargs={'commit_history': True}, daemon=True).start()
    except Exception:
        pass


def scheduled_rc_profile_update():
    global db
    if db:
        try:
            logger.info("🤖 Starting Background RC Profile Updater...")
            db.auto_update_stale_rc_profiles()
        except Exception as e:
            logger.error(f"RC Updater Error: {e}")


def watch_firebase_for_live_matches():
    global db
    if not db or not db.db:
        return

    def on_snapshot(col_snapshot, changes, read_time):
        global is_first_snapshot
        if is_first_snapshot:
            is_first_snapshot = False
            return

        has_new_match = False
        for change in changes:
            if change.type.name == 'ADDED':
                has_new_match = True

        if has_new_match:
            logger.info("📡 New Match Detected! Triggering Instant Auto-Sync...")
            threading.Thread(target=scheduled_refresh, daemon=True).start()

    try:
        db.db.collection('match_results').where('status', '==', 'approved').on_snapshot(on_snapshot)
        logger.info("📡 Instant Auto-Sync Listener Attached.")
    except Exception as e:
        logger.error(f"Failed to attach listener: {e}")


@app.before_request
def pre_request_setup():
    global db
    if db is None:
        with db_lock:
            if db is None:
                try:
                    logger.info("Initializing ThunderData safely inside worker process...")
                    db = ThunderData()
                    watch_firebase_for_live_matches()
                    logger.info("✅ Database connected.")
                except Exception as e:
                    logger.error(f"❌ FATAL: Failed to start Backend: {str(e)}")

    check_friday_auto_sync()

    if request.path == '/' or request.path == '/index':
        if not request.cookies.get('ghostmode') and not session.get('admin_logged_in'):
            if db:
                ip = request.headers.get('X-Forwarded-For', request.remote_addr)
                user_agent = request.headers.get('User-Agent', '')
                if ip:
                    db.record_page_view(ip.split(',')[0].strip(), user_agent)


scheduler = BackgroundScheduler(timezone="Australia/Brisbane")
try:
    scheduler.add_job(func=scheduled_refresh, trigger="interval", hours=12, id="scheduled_refresh")
    scheduler.add_job(func=scheduled_refresh, kwargs={'commit_history': True}, trigger="cron", day_of_week='fri', hour=6, minute=0, id="friday_sheets_sync")
    scheduler.add_job(func=scheduled_rc_profile_update, trigger="interval", hours=24, id="scheduled_rc_update")
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
except Exception:
    pass


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated_function


def super_admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        role = session.get('admin_role')
        if not session.get('admin_logged_in') or role not in ['super_admin', 'temp_super_admin']:
            return jsonify({'error': 'Super Admin Required'}), 403
        return f(*args, **kwargs)
    return decorated_function


def wait_for_sync(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if sync_lock.locked():
            logger.warning(f"⏳ Request to {request.path} is pausing to let the database sync finish...")
        with sync_lock:
            pass
        return f(*args, **kwargs)
    return decorated_function


# --- PAGES & AUTH ---

@app.route('/ghostmode')
def activate_ghost_mode():
    resp = make_response(jsonify({"success": True, "message": "👻 GHOST MODE ACTIVATED"}))
    resp.set_cookie('ghostmode', '1', max_age=60 * 60 * 24 * 365 * 10)
    return resp


@app.route('/')
def index():
    return render_template('index.html', is_admin=bool(session.get('admin_logged_in')))


@app.route('/login')
def login():
    return redirect(url_for('admin')) if session.get('admin_logged_in') else render_template('login.html')


@app.route('/player_auth')
def player_auth_page():
    return render_template('player_auth.html')


@app.route('/dashboard')
def player_dashboard():
    return render_template('dashboard.html')


@app.route('/api/cron/friday_sync', methods=['GET', 'POST'])
def cron_friday_sync():
    if not db:
        return jsonify({"success": False, "error": "Database offline"}), 500
    threading.Thread(target=scheduled_refresh, kwargs={'commit_history': True}, daemon=True).start()
    return jsonify({"success": True, "message": "Friday Google Sheets sync triggered in background."}), 200


@app.route('/api/firebase-config')
def get_firebase_config():
    return jsonify({
        "apiKey": os.environ.get("FIREBASE_API_KEY", "PASTE_YOUR_API_KEY_HERE"),
        "authDomain": os.environ.get("FIREBASE_AUTH_DOMAIN", "PASTE_YOUR_AUTH_DOMAIN_HERE"),
        "projectId": os.environ.get("FIREBASE_PROJECT_ID", "PASTE_YOUR_PROJECT_ID_HERE"),
        "storageBucket": os.environ.get("FIREBASE_STORAGE_BUCKET", "PASTE_YOUR_STORAGE_BUCKET_HERE"),
        "messagingSenderId": os.environ.get("FIREBASE_MESSAGING_SENDER_ID", "PASTE_YOUR_SENDER_ID_HERE"),
        "appId": os.environ.get("FIREBASE_APP_ID", "PASTE_YOUR_APP_ID_HERE")
    })


@app.route('/api/auth/google', methods=['POST'])
def auth_google():
    if not db:
        return jsonify({"success": False, "error": "Database offline"})
    try:
        decoded_token = fb_auth.verify_id_token(request.json.get('token'))
        email = decoded_token.get('email')
        uid = decoded_token.get('uid')
        if not email:
            return jsonify({"success": False, "error": "Invalid Token"})
    except Exception:
        return jsonify({"success": False, "error": "Invalid Token"})

    user_data = db.verify_admin_token(request.json.get('token'))
    role = user_data['role'] if user_data else 'player'
    session.permanent = True
    session['logged_in'] = True
    session['user_email'] = email
    session['user_uid'] = uid

    if role in ['admin', 'super_admin', 'temp_super_admin', 'moderator']:
        session['admin_logged_in'] = True
        session['admin_email'] = email
        session['admin_role'] = role
        db._log_audit(email, 'SESSION_START', f"Authenticated as {role}.", {})
        return jsonify({"success": True, "redirect": "/admin"})
    else:
        session['player_logged_in'] = True
        return jsonify({"success": True, "redirect": "/"})


@app.route('/api/auth/admin_login', methods=['POST'])
def auth_admin_login():
    if not db:
        return jsonify({"success": False, "error": "Database offline"})
    try:
        decoded_token = fb_auth.verify_id_token(request.json.get('token'))
        email = decoded_token.get('email')
        if not email:
            return jsonify({"success": False, "error": "Invalid Token"})
    except Exception:
        return jsonify({"success": False, "error": "Invalid Token"})

    user_data = db.verify_admin_token(request.json.get('token'))
    if not user_data:
        return jsonify({"success": False, "error": "Token Verification Failed"})

    role = user_data['role']
    if role in ['admin', 'super_admin', 'temp_super_admin', 'moderator']:
        session.permanent = True
        session['admin_logged_in'] = True
        session['admin_email'] = email
        session['admin_role'] = role
        db._log_audit(email, 'SESSION_START', f"Authenticated as {role} via Control Tower.", {})
        return jsonify({"success": True})
    else:
        return jsonify({"success": False, "error": "Access Denied. You are not an authorized Administrator."})


# ==========================================
# PASSKEY (WEBAUTHN ICLOUD KEYCHAIN SYNC) ROUTES
# ==========================================

@app.route('/api/auth/passkey/register-options', methods=['POST'])
@login_required
def passkey_register_options():
    if not db or not db.db:
        return jsonify({"error": "Database offline"}), 500

    email = (request.json or {}).get('email') or session.get('admin_email')
    if not email:
        return jsonify({"error": "Admin email required"}), 400
    email = email.lower().strip()

    challenge = _b64url_encode(os.urandom(32))
    session['webauthn_reg_challenge'] = challenge
    session['webauthn_reg_email'] = email

    user_id_b64 = _b64url_encode(hashlib.sha256(email.encode('utf-8')).digest()[:32])
    rp_id = _get_rp_id()

    return jsonify({
        "challenge": challenge,
        "rp": {
            "name": "GCTTA Admin Control Tower",
            "id": rp_id
        },
        "user": {
            "id": user_id_b64,
            "name": email,
            "displayName": f"GCTTA Admin ({email})"
        },
        "pubKeyCredParams": [
            {"type": "public-key", "alg": -7},   # ES256 (Apple iCloud Keychain / Touch ID / Face ID)
            {"type": "public-key", "alg": -257}  # RS256 (Windows Hello / Android fallback)
        ],
        "timeout": 60000,
        "attestation": "none",
        "authenticatorSelection": {
            "residentKey": "required",
            "requireResidentKey": True,
            "userVerification": "preferred"
        }
    })


@app.route('/api/auth/passkey/register-verify', methods=['POST'])
@login_required
def passkey_register_verify():
    if not db or not db.db:
        return jsonify({"success": False, "error": "Database offline"}), 500

    req = request.json or {}
    reg_response = req.get('reg_response') or req
    email = (req.get('email') or session.get('webauthn_reg_email') or session.get('admin_email', '')).lower().strip()
    expected_challenge = session.get('webauthn_reg_challenge')

    if not email or not expected_challenge:
        return jsonify({"success": False, "error": "Registration session expired. Please try again."}), 400

    try:
        resp_block = reg_response.get('response', {})
        client_data_bytes = _b64url_decode(resp_block['clientDataJSON'])
        client_data = json.loads(client_data_bytes.decode('utf-8'))

        if client_data.get('type') != 'webauthn.create':
            return jsonify({"success": False, "error": "Invalid registration response type."}), 400
        if client_data.get('challenge') != expected_challenge:
            return jsonify({"success": False, "error": "Registration challenge mismatch."}), 400

        cose_data = _extract_cose_from_attestation(resp_block['attestationObject'])
        cose_data['transports'] = resp_block.get('transports', ['internal', 'hybrid'])
        cose_data['created_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        cose_data['rp_id'] = _get_rp_id()

        doc_ref = db.db.collection('admin_users').document(email)
        doc = doc_ref.get()
        existing_creds = doc.to_dict().get('passkeys', []) if doc.exists else []
        existing_creds = [c for c in existing_creds if c.get('cred_id') != cose_data['cred_id']]
        existing_creds.append(cose_data)

        doc_ref.set({
            'email': email,
            'passkeys': existing_creds,
            'updated_at': firestore.SERVER_TIMESTAMP
        }, merge=True)

        db._log_audit(email, 'PASSKEY_REGISTERED', f"Registered synced iCloud/Multi-Device Passkey ({cose_data['cred_id'][:10]}...).", {})
        return jsonify({"success": True, "message": "Synced Passkey registered! It will automatically work across your iCloud Keychain devices."})
    except Exception as e:
        logger.error(f"Passkey registration error: {e}")
        return jsonify({"success": False, "error": str(e)}), 400


@app.route('/api/auth/passkey/generate-options', methods=['POST'])
def passkey_generate_options():
    if not db or not db.db:
        return jsonify({"error": "Database offline"}), 500

    email = ((request.json or {}).get('email') or '').lower().strip()
    if not email:
        return jsonify({"error": "Please enter your Admin Email."}), 400

    doc = db.db.collection('admin_users').document(email).get()
    if not doc.exists:
        return jsonify({"error": "No admin record found for this email."}), 404

    data = doc.to_dict()
    if data.get('role') not in ['admin', 'super_admin', 'temp_super_admin', 'moderator']:
        return jsonify({"error": "Account does not have active Admin privileges."}), 403

    passkeys = data.get('passkeys', [])
    if not passkeys:
        return jsonify({"error": "No Passkey registered for this account yet. Log in with Credentials or Bypass Code first to register your Passkey."}), 404

    challenge = _b64url_encode(os.urandom(32))
    session['webauthn_auth_challenge'] = challenge
    session['webauthn_auth_email'] = email

    allow_credentials = []
    for pk in passkeys:
        if pk.get('cred_id'):
            allow_credentials.append({
                "id": pk['cred_id'],
                "type": "public-key",
                "transports": pk.get('transports', ['internal', 'hybrid'])
            })

    return jsonify({
        "challenge": challenge,
        "rpId": _get_rp_id(),
        "allowCredentials": allow_credentials,
        "timeout": 60000,
        "userVerification": "preferred"
    })


@app.route('/api/auth/passkey/verify', methods=['POST'])
def passkey_verify():
    if not db or not db.db:
        return jsonify({"success": False, "error": "Database offline"}), 500

    req = request.json or {}
    email = (req.get('email') or session.get('webauthn_auth_email') or '').lower().strip()
    auth_response = req.get('auth_response') or {}
    expected_challenge = session.get('webauthn_auth_challenge')

    if not email or not expected_challenge:
        return jsonify({"success": False, "error": "Passkey session expired. Please try again."}), 400

    doc_ref = db.db.collection('admin_users').document(email)
    doc = doc_ref.get()
    if not doc.exists:
        return jsonify({"success": False, "error": "Admin user not found."}), 404

    data = doc.to_dict()
    role = data.get('role', 'pending')
    if role not in ['admin', 'super_admin', 'temp_super_admin', 'moderator']:
        return jsonify({"success": False, "error": "Unauthorized role."}), 403

    cred_id = auth_response.get('id') or auth_response.get('rawId')
    passkeys = data.get('passkeys', [])
    matched_cred = next((c for c in passkeys if c.get('cred_id') == cred_id), None)
    if not matched_cred and len(passkeys) == 1:
        matched_cred = passkeys[0]

    if not matched_cred:
        return jsonify({"success": False, "error": "Passkey credential not recognized."}), 403

    try:
        verified = _verify_webauthn_assertion(matched_cred, auth_response, expected_challenge, _get_rp_id())
        if not verified:
            return jsonify({"success": False, "error": "Signature verification failed."}), 403

        session.permanent = True
        session['admin_logged_in'] = True
        session['admin_email'] = email
        session['admin_role'] = role
        session.pop('webauthn_auth_challenge', None)

        db._log_audit(email, 'SESSION_START', "Authenticated via Synced Passkey.", {})

        custom_token_str = None
        try:
            try:
                fb_user = fb_auth.get_user_by_email(email)
                uid = fb_user.uid
            except Exception:
                fb_user = fb_auth.create_user(email=email)
                uid = fb_user.uid
            raw_token = fb_auth.create_custom_token(uid, {'email': email, 'role': role})
            custom_token_str = raw_token.decode('utf-8') if isinstance(raw_token, bytes) else str(raw_token)
        except Exception as token_err:
            logger.warning(f"Custom token generation fallback: {token_err}")

        return jsonify({
            "success": True,
            "token": custom_token_str,
            "redirect": "/admin"
        })
    except Exception as e:
        logger.error(f"Passkey verify error: {e}")
        return jsonify({"success": False, "error": f"Passkey verification failed: {str(e)}"}), 403


@app.route('/api/admin/change_password', methods=['POST'])
@login_required
def admin_change_password():
    email = session.get('admin_email')
    new_pass = request.json.get('password')
    if not new_pass or len(new_pass) < 6:
        return jsonify({"success": False, "error": "Password must be at least 6 characters."})
    try:
        user = fb_auth.get_user_by_email(email)
        fb_auth.update_user(user.uid, password=new_pass)
        db._log_audit(email, 'PASSWORD_CHANGE', "Admin updated their own password via Control Tower.", {})
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/auth/bypass_login', methods=['POST'])
def auth_bypass_login():
    if not db:
        return jsonify({"success": False, "error": "Database offline"}), 500

    req_email = request.json.get('email', '').lower()
    input_code = request.json.get('code', '').strip()

    if not req_email or not input_code:
        return jsonify({"success": False, "error": "Email and bypass code required."}), 400

    try:
        doc_ref = db.db.collection('admin_users').document(req_email)
        doc = doc_ref.get()

        if not doc.exists:
            return jsonify({"success": False, "error": "Unregistered emergency account."}), 403

        data = doc.to_dict()
        stored_role = data.get('role')

        if stored_role != 'temp_super_admin':
            return jsonify({"success": False, "error": "Unauthorized operational mode."}), 403

        expires_at = data.get('expires_at')

        if expires_at and datetime.datetime.now(datetime.timezone.utc) > expires_at:
            doc_ref.update({'role': 'pending', 'expires_at': firestore.DELETE_FIELD, 'bypass_pin': firestore.DELETE_FIELD})
            return jsonify({"success": False, "error": "Bypass Code session has expired."}), 403

        expected_pin = data.get('bypass_pin')

        if not expected_pin:
            base_seed = int(expires_at.timestamp()) if hasattr(expires_at, 'timestamp') else int(datetime.datetime.utcnow().timestamp())
            expected_pin = str(abs(hash(base_seed)) % 1000000).zfill(6)

        if input_code == str(expected_pin):
            doc_ref.update({'role': 'pending', 'expires_at': firestore.DELETE_FIELD, 'bypass_pin': firestore.DELETE_FIELD})

            session.permanent = True
            session['admin_logged_in'] = True
            session['admin_email'] = req_email
            session['admin_role'] = 'super_admin'
            db._log_audit(req_email, 'SESSION_START', "Emergency Bypass Clearance executed successfully.", {})
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "error": "Invalid bypass code."}), 403

    except Exception as e:
        logger.error(f"Emergency execution exception failure: {e}")
        return jsonify({"success": False, "error": "System Authentication Error"}), 500


@app.route('/logout')
def logout():
    if db and session.get('admin_email'):
        db._log_audit(session.get('admin_email'), 'SESSION_END', "Signed out.", {})
    session.clear()
    return redirect(url_for('index'))


@app.route('/admin')
def admin():
    if not session.get('admin_logged_in'):
        return redirect(url_for('login'))
    return render_template('admin.html', email=session.get('admin_email'), role=session.get('admin_role'))


# --- PUBLIC DATA APIS ---

@app.route('/api/volatility_guide')
def volatility_guide():
    return jsonify(db.get_volatility_guide()) if db else jsonify([])


@app.route('/api/clear_rc_cache')
def clear_rc_cache():
    if db:
        docs = db.db.collection('player_profiles').stream()
        for d in docs:
            try:
                d.reference.update({'recent_matches': []})
            except Exception:
                pass
        return "Cleared"
    return "DB offline"


@app.route('/api/players')
@wait_for_sync
def get_players():
    return jsonify(list(db.get_all_players().keys())) if db else jsonify([])


@app.route('/api/seasons')
@wait_for_sync
def get_seasons():
    return jsonify(db.get_seasons()) if db else jsonify([])


@app.route('/api/divisions')
@wait_for_sync
def get_divisions():
    return jsonify(db.get_divisions()) if db else jsonify([])


@app.route('/api/stats/<player_name>')
@wait_for_sync
def get_player_stats(player_name):
    if not db:
        return jsonify({"error": "Offline"}), 500
    if player_name not in db.all_players:
        for real_name in db.all_players.keys():
            if real_name.lower() == player_name.lower():
                player_name = real_name
                break

    if request.args.get('source') == 'rc':
        safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
        doc = db.db.collection('player_profiles').document(safe_id).get()
        data = doc.to_dict() if doc.exists else {}
        rc_id = data.get('ratings_central_id')

        if rc_id:
            if 'recent_matches' in data and len(data['recent_matches']) > 0 and 'date' in data['recent_matches'][0]:
                rc_stats = data
            else:
                rc_stats = db.rc_scraper.deep_scrape_profile(rc_id)
                if rc_stats:
                    db.db.collection('player_profiles').document(safe_id).set(rc_stats, merge=True)

            if rc_stats:
                hist = []
                sweeps = []
                blowouts = []
                close_calls = []
                big_swings = []
                strong_starts = []
                merciless = []
                deciders = []
                shutouts = []

                for m in rc_stats.get('recent_matches', []):
                    score_str = m.get('score', '')
                    result = m.get('result', 'Unknown')

                    parsed_sets = []
                    if score_str:
                        sets = score_str.split(',')
                        for s in sets:
                            s = s.strip()
                            if '-' in s and not s.startswith('-'):
                                parsed_sets.append(s)
                            else:
                                try:
                                    val = int(s)
                                    loser_score = abs(val)
                                    winner_score = max(11, loser_score + 2)

                                    if result == 'Win':
                                        if val >= 0:
                                            p1, p2 = winner_score, loser_score
                                        else:
                                            p1, p2 = loser_score, winner_score
                                    else:
                                        if val >= 0:
                                            p1, p2 = loser_score, winner_score
                                        else:
                                            p1, p2 = winner_score, loser_score
                                    parsed_sets.append(f"{p1}-{p2}")
                                except Exception:
                                    pass

                    new_score_str = ", ".join(parsed_sets) if parsed_sets else score_str

                    m_dict = {
                        "date": m.get('date', rc_stats.get('last_synced', '').split(' ')[0]),
                        "type": "Regular",
                        "opponent": m.get('opponent', 'Unknown'),
                        "result": result,
                        "score": new_score_str,
                        "delta": m.get('delta', 0.0)
                    }
                    hist.append(m_dict)

                    if parsed_sets:
                        p1_sets = 0
                        p2_sets = 0
                        for s in parsed_sets:
                            try:
                                p1, p2 = map(int, s.strip().split('-'))
                                if p1 > p2:
                                    p1_sets += 1
                                else:
                                    p2_sets += 1
                            except Exception:
                                pass
                        if result == "Win" and p1_sets > 0 and p2_sets == 0:
                            sweeps.append(m_dict)

                        for s in parsed_sets:
                            try:
                                p1, p2 = map(int, s.strip().split('-'))
                                if p1 >= 11 and p2 <= 2 and m_dict not in blowouts:
                                    blowouts.append(m_dict)
                                if p1 >= 11 and p2 <= 4 and m_dict not in merciless:
                                    merciless.append(m_dict)
                                if p1 >= 11 and p2 == 0 and m_dict not in shutouts:
                                    shutouts.append(m_dict)
                                if p1 >= 11 and p2 >= 10 and abs(p1 - p2) <= 2 and m_dict not in close_calls:
                                    close_calls.append(m_dict)
                            except Exception:
                                pass

                        try:
                            p1, p2 = map(int, parsed_sets[0].strip().split('-'))
                            if p1 > p2:
                                strong_starts.append(m_dict)
                        except Exception:
                            pass

                        if result == "Win" and len(parsed_sets) == 5:
                            deciders.append(m_dict)

                wins = rc_stats.get('total_wins', 0)
                losses = rc_stats.get('total_losses', 0)
                total = wins + losses
                win_rate = f"{round((wins / total) * 100, 1)}%" if total > 0 else "0%"

                bucket = {
                    "matches": total,
                    "wins": wins,
                    "losses": losses,
                    "win_rate": win_rate,
                    "match_history": hist
                }

                first_year = hist[-1].get('date', 'Unknown').split('-')[0] if hist else "Unknown"

                return jsonify({
                    "name": rc_stats.get('name', player_name),
                    "rating": rc_stats.get('rc_rating', 1500.0),
                    "rd": rc_stats.get('rc_sd', 150.0),
                    "career_matches": total,
                    "career_wins": wins,
                    "career_losses": losses,
                    "first_year": first_year,
                    "combined": bucket,
                    "regular": bucket,
                    "fillin": {"matches": 0, "wins": 0, "losses": 0, "win_rate": "0%", "match_history": []},
                    "insights": {
                        "sweeps": sweeps, "blowouts": blowouts, "close_calls": close_calls, "big_swings": big_swings,
                        "strong_starts": strong_starts, "merciless": merciless, "deciders": deciders, "shutouts": shutouts
                    }
                })

        return jsonify({"error": "No RC Data Linked"}), 404

    stats = db.get_player_stats(player_name, request.args.get('season', 'Career'), request.args.get('division', 'All'))
    if stats:
        is_admin = bool(session.get('admin_logged_in'))
        stats['is_admin_view'] = is_admin
        if not is_admin:
            stats.pop('true_rating', None)
        return jsonify(stats)
    return jsonify({"error": "Not found"}), 404


@app.route('/api/rating_history/<player_name>')
def rating_history(player_name):
    if not db:
        return jsonify([])
    clean_name = db._clean_name(player_name)

    if request.args.get('source') == 'rc':
        safe_id = re.sub(r'[^a-zA-Z0-9]', '_', clean_name).lower()
        doc = db.db.collection('player_profiles').document(safe_id).get()
        if doc.exists:
            data = doc.to_dict()
            history = []
            matches = data.get('recent_matches', [])
            current_r = data.get('rc_rating', 1500.0)

            temp_r = current_r
            for m in matches:
                delta = m.get('delta', 0.0)
                history.append({
                    "date": m.get('date', ''),
                    "opponent": m.get('opponent', 'Unknown'),
                    "result_str": m.get('score', ''),
                    "rating": round(temp_r, 1),
                    "rating_change": round(delta, 1),
                    "sd": "RC",
                    "sd_change": 0
                })
                temp_r -= delta
            return jsonify(history)
        return jsonify([])

    is_admin = bool(session.get('admin_logged_in'))
    raw_history = db.get_rating_history(clean_name, include_true_rating=is_admin)
    if not is_admin:
        sanitized = []
        for item in raw_history:
            c = dict(item)
            c.pop('true_rating', None)
            c.pop('true_rating_change', None)
            sanitized.append(c)
        return jsonify(sanitized)
    return jsonify(raw_history)


@app.route('/api/rankings/<season>/<division>')
@wait_for_sync
def get_rankings(season, division):
    if not db:
        return jsonify([])
    rankings = db.get_division_rankings(season, division, request.args.get('week', 'Latest'))
    is_admin = bool(session.get('admin_logged_in'))
    if not is_admin:
        for r in rankings:
            r.pop('true_rating', None)
    return jsonify(rankings)


@app.route('/api/week/<season>/<week>')
@wait_for_sync
def get_week_results(season, week):
    return jsonify(db.get_matches_by_week(season, week)) if db else jsonify([])


@app.route('/api/h2h')
@wait_for_sync
def get_h2h():
    if not db:
        return jsonify({"error": "Offline"}), 500
    p1 = request.args.get('p1', '')
    p2 = request.args.get('p2', '')

    if request.args.get('source') == 'rc':
        def get_rc_stats(p_name):
            safe_id = re.sub(r'[^a-zA-Z0-9]', '_', p_name).lower()
            doc = db.db.collection('player_profiles').document(safe_id).get()
            data = doc.to_dict() if doc.exists else {}
            rc_id = data.get('ratings_central_id')
            if rc_id:
                if 'recent_matches' in data and len(data['recent_matches']) > 0 and 'date' in data['recent_matches'][0]:
                    return data
                else:
                    scraped = db.rc_scraper.deep_scrape_profile(rc_id)
                    if scraped:
                        db.db.collection('player_profiles').document(safe_id).set(scraped, merge=True)
                    return scraped
            return None

        rc1 = get_rc_stats(p1)
        rc2 = get_rc_stats(p2)

        if rc1 and rc2:
            import math
            r1 = rc1.get('rc_rating', 1500.0)
            rd1 = rc1.get('rc_sd', 150.0)
            r2 = rc2.get('rc_rating', 1500.0)
            rd2 = rc2.get('rc_sd', 150.0)

            q = math.log(10) / 400
            g_rd2 = 1 / math.sqrt(1 + 3 * (q ** 2) * (rd2 ** 2) / (math.pi ** 2))
            e1 = 1 / (1 + math.pow(10, (-g_rd2 * (r1 - r2)) / 400))

            p1_win_prob = round(e1 * 100, 1)
            p2_win_prob = round((1 - e1) * 100, 1)

            delta = 16 * (1 - e1)

            history = []
            p2_last = p2.split()[-1].lower() if p2.strip() else ""
            p1_wins_h2h = 0
            p2_wins_h2h = 0

            for m in rc1.get('recent_matches', []):
                opp = m.get('opponent', '').lower()
                if p2_last and (p2_last in opp or p2.lower() in opp):
                    is_win = m.get('result') == "Win"
                    if is_win:
                        p1_wins_h2h += 1
                    else:
                        p2_wins_h2h += 1

                    history.append({
                        "date": "RC Record",
                        "p1": p1,
                        "p2": m.get('opponent', p2),
                        "s1": 3 if is_win else 0,
                        "s2": 0 if is_win else 3
                    })

            return jsonify({
                "p1_wins": p1_wins_h2h,
                "p2_wins": p2_wins_h2h,
                "total_matches": p1_wins_h2h + p2_wins_h2h,
                "p1_win_prob": p1_win_prob,
                "p2_win_prob": p2_win_prob,
                "projected_delta": round(delta, 1),
                "history": history,
                "p1_rating": round(r1, 1),
                "p1_rd": round(rd1, 1),
                "p2_rating": round(r2, 1),
                "p2_rd": round(rd2, 1),
                "p1_odds": round(1 / e1, 2) if e1 > 0 else 99.99,
                "p2_odds": round(1 / (1 - e1), 2) if (1 - e1) > 0 else 99.99
            })
        else:
            return jsonify({"error": "Both players must have a linked Ratings Central profile to compare RC data."}), 404

    h2h_data = db.get_head_to_head(p1, p2)
    if not session.get('admin_logged_in') and isinstance(h2h_data, dict):
        h2h_data.pop('p1_true_rating', None)
        h2h_data.pop('p2_true_rating', None)
    return jsonify(h2h_data)


@app.route('/api/simulate', methods=['POST'])
@wait_for_sync
def public_simulate():
    if not db:
        return jsonify({"success": False, "error": "Database Offline"}), 500
    req = request.json
    res = db.simulate_match_public(req.get('p1'), req.get('p2'), req.get('s1'), req.get('s2'), req.get('k_win'), req.get('k_loss'))
    if "error" in res:
        return jsonify({"success": False, "error": res["error"]})
    if not session.get('admin_logged_in'):
        for k in ['p1', 'p2']:
            if k in res:
                res[k].pop('old_true_rating', None)
                res[k].pop('new_true_rating', None)
                res[k].pop('true_delta', None)
    return jsonify({"success": True, "data": res})


# --- ADMIN & HUB APIS ---

@app.route('/api/create-payment-intent', methods=['POST'])
def create_payment():
    try:
        if not stripe.api_key:
            return jsonify(error="Stripe Secret Key is missing. Please configure it in the AI Studio Settings menu."), 400
        amount = int(float(request.json.get('amount', 5.00)) * 100)

        intent = stripe.PaymentIntent.create(
            amount=amount,
            currency='aud',
            automatic_payment_methods={"enabled": True}
        )
        return jsonify({'clientSecret': intent.client_secret})
    except Exception as e:
        return jsonify(error=str(e)), 403


@app.route('/api/stripe-config')
def stripe_config():
    return jsonify({
        'publicKey': os.environ.get('STRIPE_PUBLIC_KEY', 'pk_test_TYooMQauvdEDq54NiTphI7jx').strip(' "\'')
    })


@app.route('/.well-known/apple-developer-merchantid-domain-association')
def apple_pay_domain_association():
    content = os.environ.get('APPLE_PAY_DOMAIN_ASSOCIATION', '')
    if content:
        return content, 200, {'Content-Type': 'text/plain'}
    return "Domain verification not configured. Please add APPLE_PAY_DOMAIN_ASSOCIATION to env.", 404


@app.route('/api/record_donation', methods=['POST'])
def record_donation():
    return jsonify({"success": db.record_donation(request.json.get('intent_id'), request.json.get('name'), request.json.get('amount'))}) if db else jsonify({"success": False})


@app.route('/api/top_donors')
def top_donors():
    return jsonify(db.get_top_donors()) if db else jsonify([])


@app.route('/api/notices')
def get_notices():
    return jsonify(db.get_notices()) if db else jsonify([])


@app.route('/api/webhook/sms', methods=['POST', 'GET'])
def sms_webhook():
    if not db:
        return jsonify({"status": "error"}), 500
    try:
        data = request.json if request.is_json else request.form
        sender = data.get('from', 'Unknown')
        body = data.get('body', '')
        if sender and body:
            import google.cloud.firestore as firestore
            db.db.collection('sms_replies').add({'from': sender, 'body': body, 'timestamp': firestore.SERVER_TIMESTAMP, 'status': 'unread'})
        return jsonify({"status": "received"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- ADMIN: ACCOUNT MANAGEMENT ---

@app.route('/api/admin/pending_accounts')
@login_required
def get_pending_accounts():
    if not db:
        return jsonify({'success': False, 'error': 'Database offline'}), 503
    try:
        pending_users = db.admin_get_pending_accounts()
        players = [{'id': name, 'name': name} for name in db.all_players.keys()]
        return jsonify({'success': True, 'pending_users': pending_users, 'players': players})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/all_users')
@login_required
def get_all_users():
    if not db:
        return jsonify({'success': False, 'error': 'Database offline'}), 503
    try:
        profiles_ref = db.db.collection('player_profiles').stream()
        profile_map = {doc.id: doc.to_dict().get('ratings_central_id', '') for doc in profiles_ref}
        users_ref = db.db.collection('verified_users').stream()
        all_users = []
        for doc in users_ref:
            u_data = doc.to_dict()
            u_data['id'] = doc.id
            if u_data.get('linked_player_name'):
                safe_id = re.sub(r'[^a-zA-Z0-9]', '_', u_data.get('linked_player_name')).lower()
                u_data['ratings_central_id'] = profile_map.get(safe_id, '')
            all_users.append(u_data)
        return jsonify({'success': True, 'users': all_users})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/search_rc', methods=['POST'])
@login_required
def search_rc_live():
    if not db:
        return jsonify({'success': False, 'error': 'Database offline'})
    player_name = request.json.get('name', '')
    try:
        results = db.rc_scraper.search_by_name(player_name)
        return jsonify({'success': True, 'results': results})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/approve_account', methods=['POST'])
@login_required
def approve_account():
    if not db:
        return jsonify({'success': False, 'error': 'DB Offline'}), 503
    data = request.json
    user_id = data.get('user_id')
    player_name = data.get('player_name')
    rc_id = data.get('ratings_central_id')
    if not user_id or not player_name:
        return jsonify({'success': False, 'error': 'Missing user or player name'}), 400
    if db.admin_link_player_account(user_id, player_name, rc_id, session.get('admin_email')):
        return jsonify({'success': True, 'message': 'Account approved and linked!'})
    return jsonify({'success': False, 'error': 'Failed to link account.'}), 500


@app.route('/api/admin/reject_pending', methods=['POST'])
@login_required
def reject_pending():
    if not db:
        return jsonify({'success': False, 'error': 'DB Offline'}), 503
    try:
        uid = request.json.get('uid')
        db.db.collection('pending_accounts').document(uid).delete()
        try:
            fb_auth.delete_user(uid)
        except Exception:
            pass
        db._log_audit(session.get('admin_email'), 'REJECT_ACCOUNT', f"Rejected & deleted pending registration {uid}", {})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/delete_user', methods=['POST'])
@super_admin_required
def delete_user_account():
    if not db:
        return jsonify({'success': False, 'error': 'DB Offline'}), 503
    try:
        user_id = request.json.get('user_id')
        db.db.collection('verified_users').document(user_id).delete()
        try:
            fb_auth.delete_user(user_id)
        except Exception:
            pass
        db._log_audit(session.get('admin_email'), 'DELETE_USER', f"Deleted player app account {user_id}", {})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/merge_accounts', methods=['POST'])
@super_admin_required
def merge_accounts():
    if not db:
        return jsonify({'success': False, 'error': 'DB Offline'}), 503
    data = request.json
    primary_id = data.get('primary_user_id')
    duplicate_id = data.get('duplicate_user_id')
    if not primary_id or not duplicate_id or primary_id == duplicate_id:
        return jsonify({'success': False, 'error': 'Invalid account selections.'}), 400
    try:
        db.db.collection('verified_users').document(duplicate_id).delete()
        try:
            fb_auth.delete_user(duplicate_id)
        except Exception as auth_e:
            logger.warning(f"Failed to delete user from Auth: {auth_e}")
        db._log_audit(session.get('admin_email'), 'MERGE_ACCOUNTS', f"Merged duplicate user {duplicate_id} into {primary_id}", {})
        return jsonify({'success': True, 'message': 'Accounts merged. Duplicate deleted.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/admin/toggle_moderator', methods=['POST'])
@super_admin_required
def toggle_moderator():
    if not db:
        return jsonify({'success': False})
    email = request.json.get('email')
    try:
        doc_ref = db.db.collection('admin_users').document(email.lower())
        doc = doc_ref.get()
        if doc.exists and doc.to_dict().get('role') in ['moderator', 'admin', 'super_admin']:
            doc_ref.delete()
            action = "demoted"
        else:
            doc_ref.set({'email': email.lower(), 'role': 'moderator'}, merge=True)
            action = "promoted"
        db._log_audit(session.get('admin_email'), 'MODERATOR_TOGGLE', f"{action.title()} {email} to/from Moderator", {})
        return jsonify({'success': True, 'action': action})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/update_verified_link', methods=['POST'])
@login_required
def update_verified_link():
    if not db:
        return jsonify({'success': False})
    uid = request.json.get('uid')
    update_type = request.json.get('type')
    val = request.json.get('value')
    try:
        if update_type == 'player':
            db.db.collection('verified_users').document(uid).update({'linked_player_name': val})
        elif update_type == 'rc':
            user = db.db.collection('verified_users').document(uid).get().to_dict()
            if user and user.get('linked_player_name'):
                db.admin_update_player_profile(user.get('linked_player_name'), val, session.get('admin_email'))
        db._log_audit(session.get('admin_email'), 'UPDATE_ACCOUNT', f"Updated app account {uid} {update_type} to {val}", {})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/update_user_tier', methods=['POST'])
@login_required
def update_user_tier():
    if not db:
        return jsonify({'success': False})
    uid = request.json.get('uid')
    tier = request.json.get('tier', 'standard')
    try:
        db.db.collection('verified_users').document(uid).update({'tier': tier})
        user = db.db.collection('verified_users').document(uid).get().to_dict()
        if user and user.get('linked_player_name'):
            safe_id = re.sub(r'[^a-zA-Z0-9]', '_', user.get('linked_player_name')).lower()
            db.db.collection('player_profiles').document(safe_id).set({'tier': tier}, merge=True)
        db._log_audit(session.get('admin_email'), 'UPDATE_TIER', f"Updated account {uid} to {tier.upper()} tier", {})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/admin/sync_rc/<player_name>', methods=['POST'])
@login_required
def force_rc_sync(player_name):
    if not db:
        return jsonify({"success": False})
    rc_id = request.json.get('rc_id')
    if not rc_id:
        return jsonify({"success": False, "error": "No RC ID provided"})
    db.trigger_background_rc_scrape(player_name, rc_id, session.get('admin_email'))
    return jsonify({"success": True, "message": "Background scraper started!"})


# --- ADMIN: MATCHES & LOGIC ---

@app.route('/api/admin/reports')
@login_required
def get_reports():
    return jsonify(db.admin_get_reports()) if db else jsonify([])


@app.route('/api/admin/date_errors')
@login_required
def get_date_errors():
    return jsonify(db.admin_get_date_errors()) if db else jsonify([])


@app.route('/api/admin/history')
@login_required
def search_history():
    return jsonify(db.admin_search_history(request.args.get('q', ''), source_filter=request.args.get('source', 'All'))) if db else jsonify([])


@app.route('/api/admin/player_profile/<player_name>', methods=['GET'])
@login_required
def get_player_profile(player_name):
    return jsonify(db.admin_get_player_profile(player_name)) if db else jsonify({})


@app.route('/api/admin/update_player_profile', methods=['POST'])
@login_required
def update_player_profile():
    return jsonify({"success": db.admin_update_player_profile(request.json.get('player_name'), request.json.get('ratings_central_id'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/manual_match', methods=['POST'])
@login_required
def manual_match():
    return jsonify({"success": db.admin_add_manual_match(request.json.get('p1'), request.json.get('p2'), request.json.get('date'), request.json.get('scores'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/update_match', methods=['POST'])
@login_required
def update_match():
    return jsonify({"success": db.admin_update_historical_match(request.json.get('p1'), request.json.get('p2'), request.json.get('date'), request.json.get('s1'), request.json.get('s2'), request.json.get('new_date'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/bulk_fix_date', methods=['POST'])
@login_required
def bulk_fix_date():
    return jsonify({"success": db.admin_bulk_fix_date(request.json.get('season'), request.json.get('division'), request.json.get('week'), request.json.get('date'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/override_deltas', methods=['POST'])
@super_admin_required
def override_deltas():
    return jsonify({"success": db.admin_override_match_deltas(request.json.get('match_id'), request.json.get('p1_delta'), request.json.get('p2_delta'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/set_rating_scales', methods=['POST'])
@super_admin_required
def set_rating_scales():
    return jsonify({"success": db.admin_set_rating_scales(request.json.get('k_win'), request.json.get('k_loss'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/get_rating_scales', methods=['GET'])
@login_required
def get_rating_scales():
    return jsonify({"k_win": db.k_win, "k_loss": db.k_loss}) if db else jsonify({"k_win": 1.0, "k_loss": 1.4})


@app.route('/api/admin/recalculate', methods=['POST'])
@super_admin_required
def admin_recalculate():
    if not db:
        return jsonify({"success": False, "error": "Database offline"})
    return jsonify(db.admin_recalculate_ratings(session.get('admin_email')))


@app.route('/api/admin/recalculate_recent', methods=['POST'])
@super_admin_required
def admin_recalculate_recent():
    if not db:
        return jsonify({"success": False, "error": "Database offline"})
    return jsonify(db.admin_recalculate_recent(session.get('admin_email')))


@app.route('/api/admin/chaos_config', methods=['GET'])
@login_required
def get_chaos_config():
    return jsonify(db.admin_get_chaos_config()) if db else jsonify({"success": False})


@app.route('/api/admin/chaos_vote', methods=['POST'])
@super_admin_required
def chaos_vote():
    return jsonify({"success": db.admin_vote_chaos(request.json.get('weeks', []), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/chaos_clear', methods=['POST'])
@super_admin_required
def chaos_clear():
    return jsonify({"success": db.admin_clear_chaos(session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/teams', methods=['GET'])
@login_required
def admin_teams():
    return jsonify(db.admin_get_teams()) if db else jsonify([])


@app.route('/api/admin/update_team', methods=['POST'])
@login_required
def admin_update_team():
    return jsonify({"success": db.admin_update_team(request.json.get('team_id'), request.json.get('players'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/upload_schedule', methods=['POST'])
@login_required
def upload_schedule():
    if not db:
        return jsonify({"success": False, "error": "DB Offline"})
    if 'pdf' not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"})
    return jsonify(db.admin_upload_pdf_schedule(request.form.get('season', 'Unknown'), request.form.get('division', 'Unknown'), request.files['pdf'], session.get('admin_email')))


@app.route('/api/admin/upcoming_schedules', methods=['GET'])
@login_required
def get_upcoming_schedules():
    return jsonify(db.admin_get_upcoming_schedules()) if db else jsonify([])


@app.route('/api/admin/delete_schedule', methods=['POST'])
@login_required
def delete_schedule():
    return jsonify({"success": db.admin_delete_upcoming_schedule(request.json.get('schedule_id'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/export_zermelo/<tournament_id>', methods=['GET'])
@login_required
def export_zermelo(tournament_id):
    if not db:
        return "Offline", 500
    db._log_audit(session.get('admin_email'), 'EXPORT_ZERMELO', f"Downloaded Zermelo CSV for {tournament_id}", {})
    csv_data = db.generate_zermelo_csv(tournament_id)
    if not csv_data:
        return "Tournament not found", 404
    res = make_response(csv_data)
    res.headers["Content-Disposition"] = f"attachment; filename=zermelo_{tournament_id}.csv"
    res.headers["Content-type"] = "text/csv"
    return res


@app.route('/api/admin/merge', methods=['POST'])
@super_admin_required
def merge_players():
    return jsonify({"success": db.admin_merge_players(request.json.get('bad_name'), request.json.get('good_name'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/override_rating', methods=['POST'])
@super_admin_required
def override_rating():
    if not db:
        return jsonify({"success": False})
    req = request.json or {}
    return jsonify({
        "success": db.admin_override_rating(
            req.get('player_id'),
            req.get('rating'),
            req.get('sd'),
            req.get('retroactive', True),
            session.get('admin_email'),
            effective_date=req.get('effective_date')
        )
    })


@app.route('/api/admin/rating_context/<player_id>', methods=['GET'])
@login_required
def rating_context(player_id):
    return jsonify(db.get_recent_rating_context(player_id)) if db else jsonify([])


@app.route('/api/admin/bulk_pull_ratings', methods=['POST'])
@login_required
def bulk_pull_ratings():
    return jsonify(db.admin_bulk_pull_ratings(session.get('admin_email', 'Unknown'))) if db else jsonify({"success": False})


@app.route('/api/admin/force_finish_live', methods=['POST'])
@login_required
def force_finish_live():
    return jsonify({"success": db.admin_force_finish_live(request.json.get('id'), request.json.get('s1'), request.json.get('s2'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/wipe_live', methods=['POST'])
@login_required
def wipe_live():
    return jsonify({"success": db.admin_wipe_live(request.json.get('id'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/set_fixture_format', methods=['POST'])
@login_required
def set_fixture_format():
    return jsonify({"success": db.admin_set_fixture_format(request.json.get('fixture_id'), request.json.get('format_type'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/player_directory')
@login_required
def get_player_directory():
    return jsonify(db.admin_get_player_directory()) if db else jsonify([])


@app.route('/api/admin/glicko_calc', methods=['POST'])
@login_required
def glicko_calc():
    if db:
        return jsonify({"success": True, "data": db.admin_glicko_math(request.json.get('p1'), request.json.get('p2'), request.json.get('s1'), request.json.get('s2'))})
    return jsonify({"success": False})


# --- ADMIN: SYSTEM & COMMS ---

@app.route('/api/admin/add_notice', methods=['POST'])
@login_required
def add_notice():
    return jsonify({"success": db.admin_add_notice(request.json.get('title'), request.json.get('message'), request.json.get('type'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/delete_notice', methods=['POST'])
@login_required
def delete_notice():
    return jsonify({"success": db.admin_delete_notice(request.json.get('notice_id'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/delete_post', methods=['POST'])
@login_required
def delete_community_post():
    return jsonify({"success": db.admin_delete_community_post(request.json.get('post_id'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/messages')
@login_required
def get_admin_messages():
    return jsonify(db.get_admin_messages()) if db else jsonify([])


@app.route('/api/admin/add_message', methods=['POST'])
@login_required
def add_admin_message():
    return jsonify({"success": db.add_admin_message(request.json.get('message'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/contacts')
@login_required
def get_contacts():
    if not db:
        return jsonify({"emails": "", "phones": "", "preview": [], "error": "DB Offline"})
    try:
        return jsonify(db.get_contact_lists())
    except Exception as e:
        return jsonify({"emails": "", "phones": "", "preview": [], "error": str(e)})


@app.route('/api/admin/send_sms', methods=['POST'])
@login_required
def send_sms():
    if not db:
        return jsonify({"success": False, "error": "Database offline"}), 503
    req_data = request.get_json(silent=True)
    if not req_data or not req_data.get('message'):
        return jsonify({"success": False, "error": "Message cannot be empty."}), 400
    try:
        return jsonify(db.admin_send_sms_broadcast(req_data.get('message'), req_data.get('phones'), session.get('admin_email'))), 200
    except Exception as e:
        return jsonify({"success": False, "error": f"Internal Server Error: {str(e)}"}), 500


@app.route('/api/admin/sms_inbox')
@login_required
def get_sms_inbox():
    return jsonify(db.get_sms_inbox()) if db else jsonify([])


@app.route('/api/admin/donations')
@login_required
def admin_donations():
    return jsonify(db.admin_get_all_donations()) if db else jsonify([])


@app.route('/api/admin/traffic')
@login_required
def admin_traffic():
    return jsonify(db.get_traffic_stats()) if db else jsonify({'views': 0, 'uniques': 0, 'bots': 0, 'humans': 0, 'logs': []})


@app.route('/api/admin/audit_logs')
@login_required
def audit_logs():
    return jsonify(db.get_audit_logs()) if db else jsonify([])


@app.route('/api/admin/undo_action', methods=['POST'])
@super_admin_required
def undo_action():
    return jsonify({"success": db.undo_audit_action(request.json.get('log_id'), session.get('admin_email'))}) if db else jsonify({"success": False})


@app.route('/api/admin/users')
@super_admin_required
def admin_users():
    return jsonify(db.get_admin_users()) if db else jsonify([])


@app.route('/api/admin/approve_user', methods=['POST'])
@super_admin_required
def approve_user():
    return jsonify({"success": db.approve_admin(request.json.get('email'), request.json.get('action'))}) if db else jsonify({"success": False})


@app.route('/api/refresh', methods=['POST'])
@login_required
def force_refresh():
    if db:
        db._log_audit(session.get('admin_email'), 'FORCE_REFRESH', "Manually pulled from Google Sheets", {})
    threading.Thread(target=scheduled_refresh, daemon=True).start()
    return jsonify({"success": True, "status": "Refreshed"})


# --- TOURNAMENTS & HUB ---

@app.route('/api/admin/tournaments/create', methods=['POST'])
@login_required
def create_tournament():
    if not db:
        return jsonify({"success": False, "error": "DB Offline"}), 503
    return jsonify(db.admin_create_tournament(request.get_json(silent=True), session.get('admin_email')))


@app.route('/api/admin/tournaments/events/create', methods=['POST'])
@login_required
def create_event():
    if not db:
        return jsonify({"success": False, "error": "DB Offline"}), 503
    req = request.get_json(silent=True)
    return jsonify(db.admin_create_event(req.get('tournament_id'), req.get('event_data', {}), session.get('admin_email')))


@app.route('/api/hub/register', methods=['POST'])
def hub_register():
    return jsonify(db.register_player_account(request.json.get('name'), request.json.get('dob'), request.json.get('email'), request.json.get('uid'), request.json.get('estimated_rating'), request.json.get('club'))) if db else jsonify({"success": False})


@app.route('/api/hub/tournaments/check_eligibility', methods=['POST'])
def hub_check_eligibility():
    if not db:
        return jsonify({"eligible": False, "reasons": ["Database offline"]})
    req = request.get_json(silent=True)
    eligible, reasons = db.check_eligibility(req.get('uid'), req.get('event_data', {}), req.get('partner_uid'))
    return jsonify({"eligible": eligible, "reasons": reasons})


@app.route('/api/hub/forum', methods=['GET'])
def get_forum():
    return jsonify(db.get_community_feed()) if db else jsonify([])


@app.route('/api/hub/post', methods=['POST'])
def make_post():
    return jsonify({"success": db.create_community_post(request.json.get('uid'), request.json.get('name'), request.json.get('content'), request.json.get('type', 'General'), request.json.get('image_url'), request.json.get('poll_options'))}) if db else jsonify({"success": False})


@app.route('/api/hub/vote', methods=['POST'])
def vote_poll():
    return jsonify({"success": db.vote_community_poll(request.json.get('post_id'), request.json.get('option'), request.json.get('uid'))}) if db else jsonify({"success": False})


@app.route('/api/hub/comment', methods=['POST'])
def add_comment():
    return jsonify({"success": db.add_community_comment(request.json.get('post_id'), request.json.get('name'), request.json.get('content'))}) if db else jsonify({"success": False})


@app.route('/api/hub/upvote', methods=['POST'])
def toggle_upvote():
    return jsonify({"success": db.toggle_post_upvote(request.json.get('post_id'), request.json.get('uid'))}) if db else jsonify({"success": False})


@app.route('/api/hub/schedule/<player_name>', methods=['GET'])
def get_schedule(player_name):
    return jsonify(db.get_player_upcoming_schedule(player_name)) if db else jsonify([])


@app.route('/api/hub/tournament/update_cart', methods=['POST'])
def update_cart():
    return jsonify(db.process_tournament_cart(request.json.get('uid'), request.json.get('tournament_id'), request.json.get('events'), request.json.get('total'))) if db else jsonify({"success": False})


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=port)