import gspread
from google.oauth2.service_account import Credentials
import datetime
import re
import os
import json
import sys

# Ensure backend folder is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path: sys.path.append(current_dir)

try:
    from sky_engine import SkyEngine
except ImportError:
    from backend.sky_engine import SkyEngine

RESULTS_SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
DATES_SPREADSHEET_ID   = "1irddXf_SgtCpR6F7fUOoWmkvogKWowOoxEG_WWSaoHc"

class ThunderData:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.client = None
        self.sheet_results = None
        try: self.sky_engine = SkyEngine()
        except: self.sky_engine = None
        
        self.all_players = {}
        self.season_stats = {}
        self.seasons_list = []
        self.divisions_list = set()
        self.date_lookup = {} 
        self.weekly_matches = {} 
        
        try:
            creds_json = os.environ.get("GOOGLE_CREDS_JSON")
            if creds_json:
                info = json.loads(creds_json)
                self.creds = Credentials.from_service_account_info(info, scopes=self.scopes)
            else:
                if os.path.exists("credentials.json"): self.creds = Credentials.from_service_account_file("credentials.json", scopes=self.scopes)
                elif os.path.exists(os.path.join(current_dir, "credentials.json")): self.creds = Credentials.from_service_account_file(os.path.join(current_dir, "credentials.json"), scopes=self.scopes)
                else: self.creds = None
            
            if self.creds: self.client = gspread.authorize(self.creds)
        except Exception as e: print(f"❌ Error: {e}")

        if self.client:
            try: self.sheet_results = self.client.open_by_key(RESULTS_SPREADSHEET_ID)
            except: pass
        if self.sheet_results: self.refresh_data()

    def get_sky_data(self, lat=None, lon=None):
        if self.sky_engine: return self.sky_engine.get_environment_data(lat, lon)
        return {"is_day": False, "temp": 20, "condition": "Clear", "holiday": "normal"}

    def _clean_name(self, name): return " ".join(str(name).split()) if name else ""
    def _get_val(self, row, keys, default=''): 
        for k in keys: 
            if k in row: return row[k]
        return default
    def _parse_date(self, date_str):
        if not date_str: return None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try: return datetime.datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError: continue
        return None

    def _load_calculated_dates(self):
        self.date_lookup = {}
        try:
            ws = self.sheet_results.worksheet("Calculated_Dates")
            for row in ws.get_all_records():
                s, d, w, date_val = str(row.get('Season','')), str(row.get('Division','')), str(row.get('Week','')), str(row.get('Date',''))
                parsed = self._parse_date(date_val)
                if parsed: self.date_lookup[f"{s}|{d}|{w}"] = parsed.strftime("%d/%m/%Y")
        except: pass

    def refresh_data(self):
        print("⚡️ Refreshing data...")
        self.all_players = {}
        self.season_stats = {}
        self.seasons_list = ["Career"] 
        self.divisions_list = set()
        self.weekly_matches = {} 
        self._load_calculated_dates()
        
        if not self.sheet_results: return

        for worksheet in self.sheet_results.worksheets():
            title = worksheet.title
            if "Season:" not in title: continue
            season_name = title.replace("Season:", "").strip()
            self.seasons_list.append(season_name)
            self.season_stats[season_name] = {}
            if season_name not in self.weekly_matches: self.weekly_matches[season_name] = {}
            
            try:
                records = worksheet.get_all_records()
                for row in records:
                    fmt = str(self._get_val(row, ['Format', 'Match Format', 'Type'])).lower().strip()
                    if "doubles" in fmt: continue 
                    p1 = self._clean_name(self._get_val(row, ['Name 1', 'Player 1']))
                    p2 = self._clean_name(self._get_val(row, ['Name 2', 'Player 2']))
                    if not p1 and not p2: continue
                    if not p1: p1 = "Unknown"
                    if not p2: p2 = "Unknown"
                    div = str(self._get_val(row, ['Division', 'Div'], 'Unknown')).strip()
                    if div: self.divisions_list.add(div)
                    
                    status1 = str(self._get_val(row, ['PS 1', 'Pos 1'])).strip().upper()
                    status2 = str(self._get_val(row, ['PS 2', 'Pos 2'])).strip().upper()
                    p1_fill = ("S" in status1)
                    p2_fill = ("S" in status2)

                    match_date_str = ""
                    week_num = "Unknown"
                    round_val = self._get_val(row, ['Round', 'Rd', 'Week'])
                    if round_val:
                        try: week_num = int(re.search(r'\d+', str(round_val)).group())
                        except: pass
                    
                    raw_date = self._get_val(row, ['Date', 'Match Date'])
                    parsed_row_date = self._parse_date(raw_date)
                    if parsed_row_date: match_date_str = parsed_row_date.strftime("%d/%m/%Y")
                    elif week_num != "Unknown":
                        lookup = f"{season_name}|{div}|{week_num}"
                        if lookup in self.date_lookup: match_date_str = self.date_lookup[lookup]

                    s1_val = self._get_val(row, ['Sets 1', 'S1'])
                    s2_val = self._get_val(row, ['Sets 2', 'S2'])
                    try:
                        if str(s1_val).strip() == "" or str(s2_val).strip() == "": continue
                        p1_sets = int(s1_val); p2_sets = int(s2_val)
                    except: continue 

                    self._update_player_stats(self.season_stats[season_name], p1, p1_sets, p2_sets, True, p2, p1_fill, div, match_date_str, week_num, season_name)
                    self._update_player_stats(self.all_players, p1, p1_sets, p2_sets, True, p2, p1_fill, div, match_date_str, week_num, season_name)
                    self._update_player_stats(self.season_stats[season_name], p2, p1_sets, p2_sets, False, p1, p2_fill, div, match_date_str, week_num, season_name)
                    self._update_player_stats(self.all_players, p2, p1_sets, p2_sets, False, p1, p2_fill, div, match_date_str, week_num, season_name)

                    if week_num != "Unknown":
                        w_key = str(week_num)
                        if w_key not in self.weekly_matches[season_name]: self.weekly_matches[season_name][w_key] = []
                        self.weekly_matches[season_name][w_key].append({'p1': p1, 'p2': p2, 'score': f"{p1_sets}-{p2_sets}", 'division': div, 'date': match_date_str})
            except Exception as e: print(f"❌ Error loading {title}: {e}")

    def _update_player_stats(self, stat_dict, player_name, p1_sets, p2_sets, is_p1, opponent, is_fillin, division, date_str, week_num, season_name):
        if player_name not in stat_dict:
            stat_dict[player_name] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
        buckets = [stat_dict[player_name]['combined']]
        if is_fillin: buckets.append(stat_dict[player_name]['fillin'])
        else: buckets.append(stat_dict[player_name]['regular'])
        my_sets = p1_sets if is_p1 else p2_sets
        op_sets = p2_sets if is_p1 else p1_sets
        result = "Win" if my_sets > op_sets else "Loss"
        for stats in buckets:
            stats['matches'] += 1; stats['sets_won'] += my_sets; stats['sets_lost'] += op_sets
            if result == "Win": stats['wins'] += 1
            else: stats['losses'] += 1
            stats['history'].append({'season': season_name, 'week': week_num, 'date': date_str, 'opponent': opponent, 'result': result, 'score': f"{my_sets}-{op_sets}", 'type': 'Fill-in' if is_fillin else 'Regular', 'division': division})

    def get_matches_by_week(self, season, week):
        if season not in self.weekly_matches: return []
        matches = self.weekly_matches[season].get(str(week), [])
        unique = []; seen = set()
        for m in matches:
            mid = f"{sorted([m['p1'],m['p2']])[0]}|{sorted([m['p1'],m['p2']])[1]}|{m['division']}"
            if mid not in seen: seen.add(mid); unique.append(m)
        return unique

    def get_division_rankings(self, season, division, max_week=None):
        if season not in self.season_stats: return []
        ranking_list = []
        for player_name, stats in self.season_stats[season].items():
            reg_hist = [m for m in stats['regular']['history'] if m['division'] == division]
            fill_hist = [m for m in stats['fillin']['history'] if m['division'] == division]
            if max_week and str(max_week) != "All":
                try:
                    limit = int(max_week)
                    reg_hist = [m for m in reg_hist if m['week'] != "Unknown" and int(m['week']) <= limit]
                    fill_hist = [m for m in fill_hist if m['week'] != "Unknown" and int(m['week']) <= limit]
                except: pass
            if not reg_hist and not fill_hist: continue 
            def calc_summary(history):
                wins = sum(1 for m in history if m['result'] == "Win")
                return {'wins': wins, 'losses': len(history) - wins, 'matches': len(history)}
            ranking_list.append({'name': player_name, 'regular': calc_summary(reg_hist), 'fillin': calc_summary(fill_hist)})
        return ranking_list

    def get_all_player_rankings(self, season="Career"):
        data = self.all_players if season == "Career" else self.season_stats.get(season, {})
        ranking_list = []
        for player_name, stats in data.items():
            s = stats['regular']
            if s['matches'] > 0:
                ranking_list.append({'name': player_name, 'wins': s['wins'], 'losses': s['losses'], 'matches': s['matches'], 'win_rate': round((s['wins']/s['matches'])*100, 1)})
        ranking_list.sort(key=lambda x: (-x['wins'], -x['win_rate'], x['name']))
        return ranking_list

    def get_teams(self, season):
        try: return [r for r in self.sheet_results.worksheet("Teams").get_all_records() if str(r['Season']) == str(season)]
        except: return []

    def save_team(self, season, division, team_name, l1, l2, l3=""):
        try:
            ws = self.sheet_results.worksheet("Teams")
            records = ws.get_all_records()
            row = next((i+2 for i, r in enumerate(records) if str(r['Season'])==str(season) and str(r['Division'])==str(division) and str(r['Team Name'])==str(team_name)), None)
            if row: ws.update(f"D{row}:F{row}", [[l1, l2, l3]])
            else: ws.append_row([season, division, team_name, l1, l2, l3])
            return True
        except: return False
            
    def delete_team(self, season, division, team_name):
        try:
            ws = self.sheet_results.worksheet("Teams")
            records = ws.get_all_records()
            row = next((i+2 for i, r in enumerate(records) if str(r['Season'])==str(season) and str(r['Division'])==str(division) and str(r['Team Name'])==str(team_name)), None)
            if row: ws.delete_rows(row); return True
            return False
        except: return False

    def create_new_season(self, season_name):
        try:
            self.sheet_results.add_worksheet(title=f"Season: {season_name}", rows=1000, cols=20).update('A1', [["Round","Date","Table","Division","Team 1","Team 2","Pos 1","Pos 2","Player 1","Player 2","Score","S1","S2","S3","S4","S5"]])
            self.refresh_data(); return True
        except: return False

    def delete_season(self, season_name):
        try:
            self.sheet_results.del_worksheet(self.sheet_results.worksheet(f"Season: {season_name}"))
            self.refresh_data(); return True
        except: return False

    def get_review_requests(self):
        try:
            records = self.sheet_results.worksheet("Reports").get_all_records()
            for i, r in enumerate(records): r['row_id'] = i + 2 
            return records
        except: return []

    def submit_request(self, reporter, email, season, match_info, description):
        try:
            self.sheet_results.worksheet("Reports").append_row([datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), reporter, season, match_info, description, "Pending", email, ""])
            return True
        except: return False

    def update_report_status(self, row_id, new_status, reason=""):
        try: self.sheet_results.worksheet("Reports").update(f"F{row_id}:H{row_id}", [[new_status, "", reason]]); return True
        except: return False

    def run_full_audit(self): return []
    def run_player_debug(self, target_player): return []
    def get_all_players(self): return self.all_players
    def get_seasons(self): return self.seasons_list
    def get_divisions(self): return sorted(list(self.divisions_list))
    
    def get_player_stats(self, player_name, season="Career", division="All", week="All", start_date=None, end_date=None):
        data = self.all_players if season == "Career" else self.season_stats.get(season, {})
        if player_name not in data: return None
        raw = data[player_name]
        start_obj = self._parse_date(start_date) if start_date else None
        end_obj = self._parse_date(end_date) if end_date else None
        
        def format_bucket(stats):
            hist = stats['history']
            if division != "All": hist = [m for m in hist if m.get('division') == division]
            if week != "All": hist = [m for m in hist if str(m.get('week')) == str(week)]
            if start_obj or end_obj:
                hist = [m for m in hist if self._parse_date(m.get('date')) and (not start_obj or self._parse_date(m.get('date')) >= start_obj) and (not end_obj or self._parse_date(m.get('date')) <= end_obj)]
            
            wins = sum(1 for m in hist if m['result'] == "Win")
            win_rate = round((wins / len(hist)) * 100, 1) if hist else 0
            try: hist.sort(key=lambda x: datetime.datetime.strptime(x['date'], "%d/%m/%Y") if x['date'] else datetime.datetime.min)
            except: pass
            disp_hist = list(hist); disp_hist.reverse()
            return {'matches': len(hist), 'wins': wins, 'losses': len(hist)-wins, 'win_rate': f"{win_rate}%", 'sets_won': 0, 'sets_lost': 0, 'match_history': disp_hist}
        return {'name': player_name, 'regular': format_bucket(raw['regular']), 'fillin': format_bucket(raw['fillin']), 'combined': format_bucket(raw['combined'])}
    
    def get_head_to_head(self, p1, p2):
        if p1 not in self.all_players: return None
        matches = [m for m in self.all_players[p1]['combined']['history'] if m['opponent'].lower() == p2.lower()]
        matches.reverse()
        wins = sum(1 for m in matches if m['result'] == "Win")
        return {'player1': p1, 'player2': p2, 'matches': matches, 'p1_wins': wins, 'p2_wins': len(matches) - wins}