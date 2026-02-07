import firebase_admin
from firebase_admin import credentials, firestore
import os

# 1. Connect
try:
    if os.path.exists('firebase_credentials.json'):
        cred = credentials.Certificate('firebase_credentials.json')
    elif os.path.exists('backend/firebase_credentials.json'):
        cred = credentials.Certificate('backend/firebase_credentials.json')
    else:
        print("❌ Missing firebase_credentials.json")
        exit()

    try:
        app = firebase_admin.get_app()
    except ValueError:
        firebase_admin.initialize_app(cred)
        
    db = firestore.client()
    print("✅ Connected. Wiping ghost matches...")
except Exception as e:
    print(f"Error: {e}")
    exit()

# 2. Wipe 'fixtures' (The Live Dashboard)
docs = db.collection('fixtures').stream()
count = 0
for doc in docs:
    print(f"Deleting: {doc.id}")
    db.collection('fixtures').document(doc.id).delete()
    count += 1

print(f"✨ Done. Deleted {count} matches. Restart your server.")