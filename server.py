from flask import Flask, render_template, jsonify, request
try:
    from backend import ThunderData
except ImportError:
    from backend.backend import ThunderData

app = Flask(__name__, template_folder='frontend/templates')
db = ThunderData()

ADMIN_PASSWORD = "thunderadmin"

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/admin')
def admin_panel():
    return render_template('admin.html')

# --- ADMIN API ROUTES ---
@app.route('/api/admin/login', methods=['POST'])
def admin_login():
    data = request.json
    if data.get('password') == ADMIN_PASSWORD:
        return jsonify({"success": True})
    return jsonify({"success": False}), 401

@app.route('/api/admin/requests')
def get_requests():
    return jsonify(db.get_review_requests())

@app.route('/api/admin/audit')
def get_audit():
    return jsonify(db.run_full_audit())

@app.route('/api/admin/audit/<player_name>')
def get_player_audit(player_name):
    return jsonify(db.run_player_debug(player_name))

# --- PUBLIC API ROUTES ---
@app.route('/api/players')
def get_players():
    players_dict = db.get_all_players()
    player_names = list(players_dict.keys())
    player_names.sort()
    return jsonify(player_names)

@app.route('/api/seasons')
def get_seasons():
    return jsonify(db.get_seasons())

@app.route('/api/divisions')
def get_divisions():
    return jsonify(db.get_divisions())

@app.route('/api/stats/<player_name>')
def get_player_stats(player_name):
    season = request.args.get('season', 'Career')
    division = request.args.get('division', 'All')
    start_date = request.args.get('start', '')
    end_date = request.args.get('end', '')
    
    stats = db.get_player_stats(player_name, season, division, start_date, end_date)
    if stats: return jsonify(stats)
    else: return jsonify({"error": "Player not found"}), 404

@app.route('/api/compare/<p1>/<p2>')
def compare_players(p1, p2):
    data = db.get_head_to_head(p1, p2)
    if data: return jsonify(data)
    else: return jsonify({"error": "One or both players not found"}), 404

@app.route('/api/report', methods=['POST'])
def submit_report():
    data = request.json
    success = db.submit_request(data.get('reporter'), data.get('season'), data.get('match_info'), data.get('description'))
    return jsonify({"message": "Report submitted"}) if success else (jsonify({"error": "Failed"}), 500)

if __name__ == '__main__':
    app.run(debug=True, port=5001)