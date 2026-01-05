from flask import Flask, render_template, jsonify, request
import json
import os
import sys

# Ensure we can find the backend folder
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(current_dir, 'backend')
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

# Robust import for ThunderData
try:
    from backend import ThunderData
except ImportError:
    try:
        from backend.backend import ThunderData
    except ImportError:
        print("CRITICAL ERROR: Could not import ThunderData from backend.py")
        sys.exit(1)

app = Flask(__name__, template_folder='frontend/templates', static_folder='frontend/static')

# Initialize the Backend
try:
    db = ThunderData()
except Exception as e:
    print(f"Error initializing database: {e}")
    db = None

ADMIN_PASSWORD = "thunderadmin"
SCORER_PASSWORD = "tabletennis" 
SETTINGS_FILE = "settings.json"

def load_settings():
    if not os.path.exists(SETTINGS_FILE):
        return {"holiday_mode": "auto", "show_weather": True, "theme_enabled": True}
    try:
        with open(SETTINGS_FILE, 'r') as f: return json.load(f)
    except: return {"holiday_mode": "auto", "show_weather": True, "theme_enabled": True}

def save_settings(new_settings):
    with open(SETTINGS_FILE, 'w') as f: json.dump(new_settings, f)

@app.route('/')
def home():
    user_agent = request.headers.get('User-Agent', '').lower()
    is_mobile = "iphone" in user_agent or "android" in user_agent or "mobile" in user_agent
    if is_mobile:
        return render_template('mobile.html')
    return render_template('index.html')

@app.route('/admin')
def admin_panel():
    return render_template('admin.html')

@app.route('/scorer')
def scorer_page():
    return render_template('scorer.html')

# --- API ENDPOINTS ---

@app.route('/api/environment')
def get_environment():
    settings = load_settings()
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    
    if db:
        data = db.get_sky_data(lat, lon)
    else:
        data = {"is_day": True, "temp": 25, "condition": "Backend Error", "holiday": "normal"}
    
    # Admin Overrides
    if not settings.get('show_weather', True):
        data['condition'] = "Hidden"
        
    if settings.get('holiday_mode') != 'auto':
        data['holiday'] = settings.get('holiday_mode')
        if not settings.get('theme_enabled', True):
             data['holiday'] = 'normal'

    data['settings'] = settings
    return jsonify(data)

@app.route('/api/admin/settings', methods=['GET', 'POST'])
def handle_settings():
    if request.method == 'POST':
        save_settings(request.json)
        return jsonify({"success": True})
    return jsonify(load_settings())

@app.route('/api/scorer/login', methods=['POST'])
def scorer_login():
    if request.json.get('password') == SCORER_PASSWORD: return jsonify({"success": True})
    return jsonify({"success": False}), 401

@app.route('/api/admin/login', methods=['POST'])
def admin_login():
    if request.json.get('password') == ADMIN_PASSWORD: return jsonify({"success": True})
    return jsonify({"success": False}), 401

@app.route('/api/admin/requests')
def get_requests():
    return jsonify(db.get_review_requests() if db else [])

@app.route('/api/admin/update_report', methods=['POST'])
def update_report():
    data = request.json
    if db and db.update_report_status(data.get('row_id'), data.get('status'), data.get('reason', '')):
        return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/admin/create_season', methods=['POST'])
def create_season_route():
    data = request.json
    if db and db.create_new_season(data.get('name')): return jsonify({"success": True})
    return jsonify({"success": False}), 400

@app.route('/api/admin/delete_season', methods=['POST'])
def delete_season_route():
    data = request.json
    if db and db.delete_season(data.get('name')): return jsonify({"success": True})
    return jsonify({"success": False}), 400

@app.route('/api/admin/teams/<season>')
def get_teams_route(season):
    return jsonify(db.get_teams(season) if db else [])

@app.route('/api/admin/save_team', methods=['POST'])
def save_team_route():
    d = request.json
    if db and db.save_team(d.get('season'), d.get('division'), d.get('team_name'), d.get('l1'), d.get('l2'), d.get('l3', '')):
        return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/admin/delete_team', methods=['POST'])
def delete_team_route():
    d = request.json
    if db and db.delete_team(d.get('season'), d.get('division'), d.get('team_name')):
        return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/players')
def get_players():
    if not db: return jsonify([])
    return jsonify(sorted(list(db.get_all_players().keys())))

@app.route('/api/seasons')
def get_seasons():
    return jsonify(db.get_seasons() if db else [])

@app.route('/api/divisions')
def get_divisions():
    return jsonify(db.get_divisions() if db else [])

@app.route('/api/stats/<player_name>')
def get_player_stats(player_name):
    if not db: return jsonify({"error": "Backend offline"}), 500
    stats = db.get_player_stats(
        player_name, 
        request.args.get('season', 'Career'), 
        request.args.get('division', 'All'), 
        request.args.get('week', 'All'),
        request.args.get('start', ''),
        request.args.get('end', '')
    )
    if stats: return jsonify(stats)
    return jsonify({"error": "Player not found"}), 404

@app.route('/api/compare/<p1>/<p2>')
def compare_players(p1, p2):
    if not db: return jsonify({"error": "Backend offline"}), 500
    data = db.get_head_to_head(p1, p2)
    return jsonify(data) if data else (jsonify({"error": "Not found"}), 404)

@app.route('/api/report', methods=['POST'])
def submit_report():
    d = request.json
    if db and db.submit_request(d.get('reporter'), d.get('email'), d.get('season'), d.get('match_info'), d.get('description')):
        return jsonify({"message": "OK"})
    return jsonify({"error": "Failed"}), 500

@app.route('/api/week/<season>/<week>')
def get_week_matches(season, week):
    return jsonify(db.get_matches_by_week(season, week) if db else [])

@app.route('/api/rankings/<season>/<division>')
def get_rankings(season, division):
    if not db: return jsonify([])
    return jsonify(db.get_division_rankings(season, division, request.args.get('week', None)))

@app.route('/api/rankings/global/<season>')
def get_global_rankings(season):
    if not db: return jsonify([])
    return jsonify(db.get_all_player_rankings(season))

if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')