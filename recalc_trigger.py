# recalc_trigger.py
import logging
from backend.backend import ThunderData

# Setup basic logging to see output
logging.basicConfig(level=logging.INFO)

print("🚀 Starting full rating history recalculation...")
db = ThunderData()

# Run the recalculation logic we added to backend.py
result = db.admin_recalculate_ratings("terminal_trigger@gctta.com")

if result.get('success'):
    print(f"✅ Success: {result.get('message')}")
else:
    print(f"❌ Error: {result.get('error')}")