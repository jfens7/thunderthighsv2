from flask import Flask, render_template, jsonify, request
try:
    from backend import ThunderData
except ImportError:
    from backend.backend import ThunderData

app = Flask(__name__, template_folder='frontend/templates')
db = ThunderData()

ADMIN_PASSWORD = "thunderadmin"
SCORER_PASSWORD = "tabletennis" 

@app.route('/')
def home(): return render_template('index.html')

@app.route('/admin')
def admin_panel(): return render_template('admin.html')

@app.route('/scorer')
def scorer_page(): return render_template('scorer.html')

# --- NEW ENVIRONMENT ENDPOINT ---
@app.route('/api/environment')
def get_environment():
    # Returns: {is_day, temp, condition, moon_phase, moon_icon_code, star_visibility}
    return jsonify(db.get_sky_data())

@app.route('/api/scorer/login', methods=['POST'])
def scorer_login():
    if request.json.get('password') == SCORER_PASSWORD: return jsonify({"success": True})
    return jsonify({"success": False}), 401

@app.route('/api/admin/login', methods=['POST'])
def admin_login():
    if request.json.get('password') == ADMIN_PASSWORD: return jsonify({"success": True})
    return jsonify({"success": False}), 401

@app.route('/api/admin/requests')
def get_requests(): return jsonify(db.get_review_requests())

@app.route('/api/admin/update_report', methods=['POST'])
def update_report():
    data = request.json
    if db.update_report_status(data.get('row_id'), data.get('status'), data.get('reason', '')): 
        return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/admin/create_season', methods=['POST'])
def create_season_route():
    data = request.json
    success = db.create_new_season(data.get('name'))
    if success: return jsonify({"success": True})
    else: return jsonify({"success": False, "error": "Season already exists or failed"}), 400

@app.route('/api/admin/delete_season', methods=['POST'])
def delete_season_route():
    data = request.json
    success = db.delete_season(data.get('name'))
    if success: return jsonify({"success": True})
    else: return jsonify({"success": False}), 400

@app.route('/api/admin/teams/<season>')
def get_teams_route(season):
    return jsonify(db.get_teams(season))

@app.route('/api/admin/save_team', methods=['POST'])
def save_team_route():
    d = request.json
    success = db.save_team(d.get('season'), d.get('division'), d.get('team_name'), d.get('l1'), d.get('l2'), d.get('l3', ''))
    return jsonify({"success": success})

@app.route('/api/admin/delete_team', methods=['POST'])
def delete_team_route():
    d = request.json
    success = db.delete_team(d.get('season'), d.get('division'), d.get('team_name'))
    return jsonify({"success": success})

@app.route('/api/admin/audit')
def get_audit(): return jsonify(db.run_full_audit())

@app.route('/api/admin/audit/<player_name>')
def get_player_audit(player_name): return jsonify(db.run_player_debug(player_name))

@app.route('/api/players')
def get_players():
    players = list(db.get_all_players().keys())
    players.sort()
    return jsonify(players)

@app.route('/api/seasons')
def get_seasons(): return jsonify(db.get_seasons())

@app.route('/api/divisions')
def get_divisions(): return jsonify(db.get_divisions())

@app.route('/api/stats/<player_name>')
def get_player_stats(player_name):
    stats = db.get_player_stats(player_name, request.args.get('season', 'Career'), request.args.get('division', 'All'), request.args.get('week', 'All'), request.args.get('start', ''), request.args.get('end', ''))
    if stats: return jsonify(stats)
    else: return jsonify({"error": "Player not found"}), 404

@app.route('/api/compare/<p1>/<p2>')
def compare_players(p1, p2):
    data = db.get_head_to_head(p1, p2)
    return jsonify(data) if data else (jsonify({"error": "Not found"}), 404)

@app.route('/api/report', methods=['POST'])
def submit_report():
    d = request.json
    success = db.submit_request(d.get('reporter'), d.get('email'), d.get('season'), d.get('match_info'), d.get('description'))
    return jsonify({"message": "OK"}) if success else (jsonify({"error": "Failed"}), 500)

@app.route('/api/week/<season>/<week>')
def get_week_matches(season, week):
    matches = db.get_matches_by_week(season, week)
    return jsonify(matches)

@app.route('/api/rankings/<season>/<division>')
def get_rankings(season, division):
    max_week = request.args.get('week', None)
    return jsonify(db.get_division_rankings(season, division, max_week))

@app.route('/api/rankings/global/<season>')
def get_global_rankings(season):
    return jsonify(db.get_all_player_rankings(season))

if __name__ == '__main__':
    app.run(debug=True, port=5001)