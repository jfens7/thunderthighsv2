import firebase_admin
from firebase_admin import credentials, auth
import sys
import os

print("🚨 ADMIN PASSWORD RESET TOOL")

possible_paths = [
    "firebase-service-account.json",
    "firebase_credentials.json",
    "credentials.json",
    "backend/firebase_credentials.json",
    "backend/credentials.json"
]

creds_path = None
for path in possible_paths:
    if os.path.exists(path):
        creds_path = path
        break

if not creds_path:
    print("❌ CRITICAL ERROR: Firebase credentials file not found.")
    sys.exit(1)

print(f"✅ Found credentials at: {creds_path}")

try:
    if not firebase_admin._apps:
        cred = credentials.Certificate(creds_path)
        firebase_admin.initialize_app(cred)
except Exception as e:
    print(f"❌ Firebase initialization failed: {e}")
    sys.exit(1)

email = input("🔑 Enter admin email to reset password: ").strip()
if not email:
    sys.exit(1)

new_password = input("🔒 Enter new password: ").strip()
if len(new_password) < 6:
    print("❌ Password must be at least 6 characters long.")
    sys.exit(1)

try:
    user = auth.get_user_by_email(email)
    auth.update_user(user.uid, password=new_password)
    print(f"\n✅ SUCCESS! Password for {email} successfully updated.")
    print("You can now log in using the normal credentials tab with your new password.")
except auth.UserNotFoundError:
    print(f"\n❌ Error: No user found with email {email}")
except Exception as e:
    print(f"\n❌ Error updating password: {e}")
