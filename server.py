from flask import Flask, render_template, jsonify, request
import json
import os
import sys
import traceback

# --- PATH SETUP ---
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(current_dir, 'backend')

if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

# --- IMPORTS ---
try:
    from backend import ThunderData
except ImportError:
    try:
        from backend.backend import ThunderData
    except ImportError as e:
        print(f"CRITICAL ERROR: Could not import ThunderData. {e}")
        sys.exit(1)

try:
    from team_generator import TeamGenerator
    print("✅ TeamGenerator loaded.")
except ImportError:
    TeamGenerator = None

try:
    from firebase_sync import FirebaseSyncer
    syncer = FirebaseSyncer()
except ImportError:
    syncer = None

app = Flask(__name__, template_folder='frontend/templates', static_folder='frontend/static')

try:
    db = ThunderData()
except Exception as e:
    print(f"Error initializing database: {e}")
    db = None

ADMIN_PASSWORD = "thunderadmin"
SCORER_PASSWORD = "tabletennis" 
SETTINGS_FILE = "settings.json"

# --- HELPER: COLUMN MAPPER ---
DIV_COLUMN_MAP = {
    "Division 1": 0, "Division 2": 3, "Division 3": 6, "Division 4": 9, "Division 5": 12
}

def load_settings():
    if not os.path.exists(SETTINGS_FILE): return {"holiday_mode": "auto", "show_weather": True}
    with open(SETTINGS_FILE, 'r') as f: return json.load(f)

def save_settings(new_settings):
    with open(SETTINGS_FILE, 'w') as f: json.dump(new_settings, f)

# --- ROUTES ---

@app.route('/')
def home():
    user_agent = request.headers.get('User-Agent', '').lower()
    if "mobile" in user_agent or "iphone" in user_agent:
        return render_template('mobile.html')
    return render_template('index.html')

@app.route('/admin')
def admin_panel(): return render_template('admin.html')

@app.route('/scorer')
def scorer_page(): return render_template('scorer.html')

@app.route('/api/environment')
def get_environment():
    settings = load_settings()
    if db: data = db.get_sky_data(request.args.get('lat'), request.args.get('lon'))
    else: data = {"is_day": True, "temp": 25, "condition": "Backend Error"}
    if not settings.get('show_weather', True): data['condition'] = "Hidden"
    if settings.get('holiday_mode') != 'auto': data['holiday'] = settings.get('holiday_mode')
    data['settings'] = settings
    return jsonify(data)

@app.route('/api/admin/settings', methods=['POST', 'GET'])
def handle_settings():
    if request.method == 'POST':
        save_settings(request.json)
        return jsonify({"success": True})
    return jsonify(load_settings())

@app.route('/api/scorer/login', methods=['POST'])
def scorer_login(): return jsonify({"success": request.json.get('password') == SCORER_PASSWORD})

@app.route('/api/admin/login', methods=['POST'])
def admin_login(): return jsonify({"success": request.json.get('password') == ADMIN_PASSWORD})

# --- GENERATOR LOGIC ---
@app.route('/api/admin/generate-teams', methods=['POST'])
def api_generate_teams():
    if not TeamGenerator: return jsonify({"error": "Generator module missing"}), 500
    if not db: return jsonify({"error": "DB not connected"}), 500
    try:
        req_data = request.json
        target_division = req_data.get('division', 'Division 1')
        sheet_name = "division position summer 2026"
        try: ws = db.sheet_results.worksheet(sheet_name)
        except: return jsonify({"error": f"Sheet '{sheet_name}' not found"}), 404
        all_values = ws.get_all_values()
        
        # Build Ratings Map
        ratings_map = {} 
        start_row_index = -1
        name_col_idx = 5; rating_col_idx = 7
        for i, row in enumerate(all_values):
            row_str = [str(c).strip() for c in row]
            if "Name" in row_str and "Rating" in row_str:
                start_row_index = i + 1
                name_col_idx = row_str.index("Name")
                try: rating_col_idx = row_str.index("Rating")
                except: rating_col_idx = name_col_idx + 2
                break
        
        if start_row_index != -1:
            for i in range(start_row_index, len(all_values)):
                row = all_values[i]
                if len(row) <= rating_col_idx: continue
                p_name = str(row[name_col_idx]).strip()
                p_rating_raw = str(row[rating_col_idx]).strip()
                if p_name and p_rating_raw:
                    try:
                        clean_rating = p_rating_raw.split('+')[0].split(' ')[0].strip()
                        ratings_map[p_name.lower()] = { "name": p_name, "rating": float(clean_rating), "id": f"P{i}" }
                    except: continue

        # Get Players
        target_col_idx = DIV_COLUMN_MAP.get(target_division, 0)
        active_players = []; requests = []
        scan_limit = start_row_index if start_row_index > 0 else 45
        
        for i in range(1, scan_limit):
            row = all_values[i]
            if len(row) <= target_col_idx: continue
            candidate_name = str(row[target_col_idx]).strip()
            if candidate_name and "Div" not in candidate_name and "info" not in candidate_name and "Requests" not in candidate_name:
                stats = ratings_map.get(candidate_name.lower())
                if stats: active_players.append(stats)
                else: active_players.append({ "name": candidate_name, "rating": 1000.0, "id": f"TEMP_{i}" })
            for cell in row:
                cell_str = str(cell).strip()
                if "/" in cell_str and len(cell_str) < 50 and cell_str not in requests: requests.append(cell_str)

        if not active_players: return jsonify({"error": f"No players found for {target_division}"}), 400
        result = TeamGenerator(active_players, requests, team_size=2)
        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# --- CORE API ENDPOINTS ---

@app.route('/api/players')
def get_players(): return jsonify(sorted(list(db.get_all_players().keys()))) if db else jsonify([])

@app.route('/api/seasons')
def get_seasons(): return jsonify(db.get_seasons() if db else [])

@app.route('/api/divisions')
def get_divisions(): return jsonify(db.get_divisions() if db else [])

@app.route('/api/stats/<player_name>')
def get_player_stats(player_name):
    if not db: return jsonify({"error": "Offline"}), 500
    s = db.get_player_stats(player_name, request.args.get('season','Career'), request.args.get('division','All'), request.args.get('week','All'), request.args.get('start'), request.args.get('end'))
    return jsonify(s) if s else (jsonify({"error": "Not found"}), 404)

# --- MISSING ROUTES ADDED HERE ---

@app.route('/api/week/<season>/<week>')
def get_week_matches(season, week):
    """Returns matches for the Results Tab"""
    return jsonify(db.get_matches_by_week(season, week)) if db else jsonify([])

@app.route('/api/compare/<p1>/<p2>')
def compare_players(p1, p2):
    """Returns Head-to-Head stats"""
    return jsonify(db.get_head_to_head(p1, p2)) if db else jsonify({})

# ---------------------------------

@app.route('/api/rankings/<season>/<division>')
def get_rankings(season, division):
    return jsonify(db.get_division_rankings(season, division, request.args.get('week'))) if db else jsonify([])

@app.route('/api/rankings/global/<season>')
def get_global_rankings(season):
    return jsonify(db.get_all_player_rankings(season)) if db else jsonify([])

@app.route('/api/report', methods=['POST'])
def submit_report():
    d = request.json
    return jsonify({"message": "OK"}) if db and db.submit_request(d.get('reporter'), d.get('email'), d.get('season'), d.get('match_info'), d.get('description')) else jsonify({"error":"Fail"})

# --- ADMIN ROUTES ---
@app.route('/api/admin/requests')
def get_requests(): return jsonify(db.get_review_requests() if db else [])

@app.route('/api/admin/update_report', methods=['POST'])
def update_report():
    d = request.json
    return jsonify({"success": db.update_report_status(d.get('row_id'), d.get('status'), d.get('reason',''))}) if db else jsonify({"success":False})

@app.route('/api/admin/teams/<season>')
def get_teams_route(season): return jsonify(db.get_teams(season) if db else [])

@app.route('/api/admin/save_team', methods=['POST'])
def save_team_route():
    d = request.json
    return jsonify({"success": db.save_team(d.get('season'), d.get('division'), d.get('team_name'), d.get('l1'), d.get('l2'), d.get('l3',''))}) if db else jsonify({"success":False})

@app.route('/api/admin/delete_team', methods=['POST'])
def delete_team_route():
    d = request.json
    return jsonify({"success": db.delete_team(d.get('season'), d.get('division'), d.get('team_name'))}) if db else jsonify({"success":False})

@app.route('/api/admin/find_duplicates', methods=['GET'])
def find_dupes(): return jsonify(db.get_potential_duplicates()) if db else jsonify([])

@app.route('/api/admin/resolve_duplicate', methods=['POST'])
def resolve_dupe():
    d = request.json
    if d['action'] == 'merge': return jsonify({"success": db.add_alias(d['p1'], d['p2'])})
    if d['action'] == 'rename': return jsonify({"success": db.rename_player_roster(d['p1'], d['p2'])})
    return jsonify({"success": False})

@app.route('/api/admin/sync_firebase', methods=['POST'])
def run_firebase_sync(): return jsonify(syncer.sync_all()) if syncer else jsonify({"success":False})

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')