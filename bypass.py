# bypass.py
import os
import sys
import time
import random
import datetime
import firebase_admin
from firebase_admin import credentials, firestore

print("🚨 INITIALIZING AIR-GAPPED EMERGENCY BYPASS...")

# Locate your Firebase service account credentials
creds_path = None
possible_paths = [
    "firebase_credentials.json",
    "backend/firebase_credentials.json",
    "credentials.json",
    "backend/credentials.json"
]

for path in possible_paths:
    if os.path.exists(path):
        creds_path = path
        break

if not creds_path:
    print("❌ CRITICAL ERROR: Firebase credentials file not found.")
    sys.exit(1)

print(f"✅ Found credentials at: {creds_path}")

# Initialize Firebase app
try:
    if not firebase_admin._apps:
        cred = credentials.Certificate(creds_path)
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"❌ Firebase initialization failed: {e}")
    sys.exit(1)

# Prompt for the target admin email to elevate
target_email = input("🔑 Enter registered Admin Email to elevate: ").strip().lower()

if not target_email or "@" not in target_email:
    print("❌ Invalid email address provided.")
    sys.exit(1)

# Generate a random 6-digit pin
bypass_pin = str(random.randint(100000, 999999))

# Calculate Expiry (1 hour from current UTC time)
expiry_utc = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
epoch_timestamp = int(expiry_utc.timestamp())

# Determine the deterministic PIN that server.py will calculate based on epoch timestamp
# Matches the math: str(abs(hash(epoch_timestamp)) % 1000000).zfill(6)
calculated_pin = str(abs(hash(epoch_timestamp)) % 1000000).zfill(6)

print("\n⚙️ Synchronizing emergency parameters with Firestore...")

try:
    # Directly set the emergency bypass credentials in admin_users
    db.collection('admin_users').document(target_email).set({
        'email': target_email,
        'role': 'temp_super_admin',
        'expires_at': expiry_utc
    }, merge=True)
    
    print("\n" + "="*55)
    print(f"🚀 EMERGENCY ACCESS GRANTED FOR: {target_email}")
    print(f"🎟️  EMERGENCY BYPASS PIN: 【 {calculated_pin} 】")
    print(f"⏳ TTL: Valid for 1 hour (expires at {expiry_utc.strftime('%H:%M:%S UTC')})")
    print("="*55)
    print("Instructions:")
    print("1. Navigate to your /login admin portal.")
    print("2. Select the 'Bypass Code' tab.")
    print("3. Enter your email and the 6-digit PIN above.")
    print("="*55)

except Exception as e:
    print(f"❌ Failed writing bypass parameters to Firestore: {e}")