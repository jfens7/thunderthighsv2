import gspread
from google.oauth2.service_account import Credentials
import datetime
import re
import os
import json

# ==========================================
# 👇 SPREADSHEET CONFIGURATION 👇
# ==========================================
RESULTS_SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
DATES_SPREADSHEET_ID   = "1irddXf_SgtCpR6F7fUOoWmkvogKWowOoxEG_WWSaoHc"
# ==========================================

class ThunderData:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.client = None
        self.sheet_results = None
        self.sheet_dates = None
        
        # Initialize variables so they exist even if loading fails
        self.all_players = {}
        self.season_stats = {}
        self.season_starts = {} 
        self.seasons_list = []
        self.divisions_list = set()
        
        try:
            # Check for Render environment variable first, then local file
            creds_json = os.environ.get("GOOGLE_CREDS_JSON")
            if creds_json:
                info = json.loads(creds_json)
                self.creds = Credentials.from_service_account_info(info, scopes=self.scopes)
            else:
                self.creds = Credentials.from_service_account_file("credentials.json", scopes=self.scopes)
            
            self.client = gspread.authorize(self.creds)
        except Exception as e:
            print(f"❌ Error: Could not load credentials. {e}")
            return

        if self.client:
            try:
                self.sheet_results = self.client.open_by_key(RESULTS_SPREADSHEET_ID)
                print(f"⚡️ Connected to Results: '{self.sheet_results.title}'")
                
                self.sheet_dates = self.client.open_by_key(DATES_SPREADSHEET_ID)
                print(f"📅 Connected to Dates: '{self.sheet_dates.title}'")
                
                self._ensure_review_tab()
            except Exception as e:
                print(f"❌ Error connecting to sheets: {e}")
        
        if self.sheet_results:
            self.refresh_data()

    def _ensure_review_tab(self):
        try:
            try:
                self.sheet_results.worksheet("Review Requests")
            except gspread.WorksheetNotFound:
                ws = self.sheet_results.add_worksheet(title="Review Requests", rows=100, cols=6)
                ws.append_row(["Timestamp", "Reporter Name", "Season", "Match/Issue", "Description", "Status"])
        except Exception as e:
            print(f"⚠️ Could not check/create Review tab: {e}")

    def _clean_name(self, name):
        if not name: return ""
        return " ".join(str(name).split())

    def _get_val(self, row, keys, default=''):
        for key in keys:
            if key in row: return row[key]
        return default

    def _parse_date(self, date_str):
        if not date_str: return None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                return datetime.datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _load_season_dates(self):
        print("   -> Loading Season Dates...")
        self.season_starts = {}
        if not self.sheet_dates: return
        try:
            ws = self.sheet_dates.get_worksheet(0)
            records = ws.get_all_records()
            for row in records:
                season = self._get_val(row, ['Season', 'Season Name', 'Name'])
                start_str = self._get_val(row, ['Start Date', 'Start', 'Date', 'First Round'])
                if season and start_str:
                    date_obj = self._parse_date(start_str)
                    if date_obj:
                        clean_season = season.replace("Season:", "").strip()
                        self.season_starts[clean_season] = date_obj
        except Exception as e:
            print(f"❌ Error loading dates: {e}")

    def refresh_data(self):
        print("⚡️ Refreshing data...")
        self.all_players = {}
        self.season_stats = {}
        self.seasons_list = ["Career"] 
        self.divisions_list = set()
        self._load_season_dates()
        
        if not self.sheet_results: return

        for worksheet in self.sheet_results.worksheets():
            title = worksheet.title
            if "Season:" not in title: continue
            
            season_name = title.replace("Season:", "").strip()
            self.seasons_list.append(season_name)
            self.season_stats[season_name] = {}
            season_start_date = self.season_starts.get(season_name)
            
            try:
                records = worksheet.get_all_records()
                for row in records:
                    fmt = str(self._get_val(row, ['Format', 'Match Format', 'Type'])).lower().strip()
                    if "doubles" in fmt: continue 

                    p1 = self._clean_name(self._get_val(row, ['Name 1', 'Player 1']))
                    p2 = self._clean_name(self._get_val(row, ['Name 2', 'Player 2']))
                    
                    if not p1 and not p2: continue
                    if not p1: p1 = "Unknown Player"
                    if not p2: p2 = "Unknown Opponent"

                    div = str(self._get_val(row, ['Division', 'Div'], 'Unknown')).strip()
                    if div: self.divisions_list.add(div)

                    status1 = str(self._get_val(row, ['PS 1', 'Pos 1'])).strip().upper()
                    status2 = str(self._get_val(row, ['PS 2', 'Pos 2'])).strip().upper()
                    p1_fill = ("S" in status1)
                    p2_fill = ("S" in status2)

                    match_date_str = ""
                    raw_date = self._get_val(row, ['Date', 'Match Date'])
                    if raw_date:
                        match_date_str = str(raw_date)
                    elif season_start_date:
                        round_val = self._get_val(row, ['Round', 'Rd', 'Week'])
                        try:
                            r_num = int(re.search(r'\d+', str(round_val)).group())
                            calculated_date = season_start_date + datetime.timedelta(weeks=r_num - 1)
                            match_date_str = calculated_date.strftime("%d/%m/%Y")
                        except: pass

                    s1_val = self._get_val(row, ['Sets 1', 'S1'])
                    s2_val = self._get_val(row, ['Sets 2', 'S2'])

                    try:
                        if str(s1_val).strip() == "" or str(s2_val).strip() == "": continue
                        p1_sets = int(s1_val)
                        p2_sets = int(s2_val)
                    except ValueError: continue 

                    self._update_player_stats(self.season_stats[season_name], p1, p1_sets, p2_sets, True, p2, p1_fill, div, match_date_str)
                    self._update_player_stats(self.all_players, p1, p1_sets, p2_sets, True, p2, p1_fill, div, match_date_str)
                    self._update_player_stats(self.season_stats[season_name], p2, p1_sets, p2_sets, False, p1, p2_fill, div, match_date_str)
                    self._update_player_stats(self.all_players, p2, p1_sets, p2_sets, False, p1, p2_fill, div, match_date_str)

            except Exception as e: print(f"❌ Error loading {title}: {e}")

    def _update_player_stats(self, stat_dict, player_name, p1_sets, p2_sets, is_p1, opponent, is_fillin, division, date_str):
        if player_name not in stat_dict:
            stat_dict[player_name] = {
                'regular':  {'matches': 0, 'wins': 0, 'losses': 0, 'sets_won': 0, 'sets_lost': 0, 'history': []},
                'fillin':   {'matches': 0, 'wins': 0, 'losses': 0, 'sets_won': 0, 'sets_lost': 0, 'history': []},
                'combined': {'matches': 0, 'wins': 0, 'losses': 0, 'sets_won': 0, 'sets_lost': 0, 'history': []}
            }
        
        buckets = [stat_dict[player_name]['combined']]
        if is_fillin: buckets.append(stat_dict[player_name]['fillin'])
        else: buckets.append(stat_dict[player_name]['regular'])

        if p1_sets == 0 and p2_sets == 0: return

        my_sets = p1_sets if is_p1 else p2_sets
        op_sets = p2_sets if is_p1 else p1_sets
        result = "Win" if my_sets > op_sets else "Loss"

        for stats in buckets:
            stats['matches'] += 1
            stats['sets_won'] += my_sets
            stats['sets_lost'] += op_sets
            if result == "Win": stats['wins'] += 1
            else: stats['losses'] += 1
            
            stats['history'].append({
                'date': date_str,
                'opponent': opponent,
                'result': result,
                'score': f"{my_sets}-{op_sets}",
                'type': 'Fill-in' if is_fillin else 'Regular',
                'division': division
            })

    # --- ADMIN FEATURES ---
    def get_review_requests(self):
        if not self.sheet_results: return []
        try:
            ws = self.sheet_results.worksheet("Review Requests")
            return ws.get_all_records()
        except: return []

    def submit_request(self, reporter, season, match_info, description):
        if not self.sheet_results: return False
        try:
            ws = self.sheet_results.worksheet("Review Requests")
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ws.append_row([timestamp, reporter, season, match_info, description, "Pending"])
            return True
        except: return False

    def run_full_audit(self):
        issues = []
        if not self.sheet_results: return []
        for worksheet in self.sheet_results.worksheets():
            if "Season:" not in worksheet.title: continue
            season_name = worksheet.title
            records = worksheet.get_all_records()
            for i, row in enumerate(records, start=2):
                fmt = str(self._get_val(row, ['Format', 'Match Format'])).lower().strip()
                if "doubles" in fmt: continue 
                p1 = str(self._get_val(row, ['Name 1', 'Player 1'])).strip()
                p2 = str(self._get_val(row, ['Name 2', 'Player 2'])).strip()
                if p1 and not p2: issues.append({'season': season_name, 'row': i, 'type': 'Missing Opponent', 'details': f"{p1} vs [BLANK]"})
                if p2 and not p1: issues.append({'season': season_name, 'row': i, 'type': 'Missing Player', 'details': f"[BLANK] vs {p2}"})
                s1_raw = self._get_val(row, ['Sets 1', 'S1'])
                s2_raw = self._get_val(row, ['Sets 2', 'S2'])
                if str(s1_raw).strip() == "" or str(s2_raw).strip() == "":
                    if p1 or p2: issues.append({'season': season_name, 'row': i, 'type': 'Empty Score', 'details': f"{p1} vs {p2} has no score."})
                    continue
                try: int(s1_raw); int(s2_raw)
                except ValueError: issues.append({'season': season_name, 'row': i, 'type': 'Invalid Score', 'details': f"Values: '{s1_raw}' - '{s2_raw}'"})
        return issues

    def run_player_debug(self, target_player):
        logs = []
        target = target_player.lower().strip()
        if not self.sheet_results: return []
        for worksheet in self.sheet_results.worksheets():
            if "Season:" not in worksheet.title: continue
            season_name = worksheet.title.replace("Season:", "").strip()
            records = worksheet.get_all_records()
            for i, row in enumerate(records, start=2):
                p1 = str(self._get_val(row, ['Name 1', 'Player 1'])).strip()
                p2 = str(self._get_val(row, ['Name 2', 'Player 2'])).strip()
                if target not in p1.lower() and target not in p2.lower(): continue
                fmt = str(self._get_val(row, ['Format', 'Match Format'])).lower().strip()
                if "doubles" in fmt:
                    logs.append({'row': i, 'season': season_name, 'status': 'SKIPPED', 'reason': 'Doubles Match'})
                    continue
                s1_raw = self._get_val(row, ['Sets 1', 'S1'])
                s2_raw = self._get_val(row, ['Sets 2', 'S2'])
                if str(s1_raw).strip() == "" or str(s2_raw).strip() == "":
                    logs.append({'row': i, 'season': season_name, 'status': 'SKIPPED', 'reason': f"Empty Score (Sets: '{s1_raw}'-'{s2_raw}')"})
                    continue
                try:
                    int(s1_raw); int(s2_raw)
                    opponent = p2 if target in p1.lower() else p1
                    logs.append({'row': i, 'season': season_name, 'status': 'ACCEPTED', 'reason': f"vs {opponent} ({s1_raw}-{s2_raw})"})
                except ValueError:
                    logs.append({'row': i, 'season': season_name, 'status': 'ERROR', 'reason': f"Invalid Score Format ('{s1_raw}')"})
        return logs

    # --- GETTERS ---
    def get_all_players(self): 
        return self.all_players if self.all_players else {}
    
    def get_seasons(self): return self.seasons_list
    def get_divisions(self): return sorted(list(self.divisions_list))
    
    # --- UPDATED STATS GETTER WITH DATE FILTER ---
    def get_player_stats(self, player_name, season="Career", division="All", start_date=None, end_date=None):
        dataset = self.all_players if season == "Career" or season not in self.season_stats else self.season_stats[season]
        if player_name not in dataset: return None
        raw_data = dataset[player_name]
        
        # Convert filter strings to date objects
        start_obj = self._parse_date(start_date) if start_date else None
        end_obj = self._parse_date(end_date) if end_date else None
        
        def format_bucket(stats):
            hist = stats['history']
            
            # 1. Filter by Division
            if division != "All": hist = [m for m in hist if m.get('division') == division]
            
            # 2. Filter by Date
            if start_obj or end_obj:
                filtered = []
                for m in hist:
                    match_date = self._parse_date(m.get('date'))
                    if not match_date: continue 
                    if start_obj and match_date < start_obj: continue
                    if end_obj and match_date > end_obj: continue
                    filtered.append(m)
                hist = filtered

            # 3. Recalculate Totals based on filtered history
            matches = len(hist)
            wins = sum(1 for m in hist if m['result'] == "Win")
            s_won = 0; s_lost = 0
            for m in hist:
                try: parts = m['score'].split('-'); s_won += int(parts[0]); s_lost += int(parts[1])
                except: pass
            win_rate = round((wins / matches) * 100, 1) if matches > 0 else 0
            
            try:
                hist.sort(key=lambda x: datetime.datetime.strptime(x['date'], "%d/%m/%Y") if x['date'] else datetime.datetime.min)
            except: pass
            
            disp_hist = list(hist)
            disp_hist.reverse()
            return {'matches': matches, 'wins': wins, 'losses': matches - wins, 'win_rate': f"{win_rate}%", 'sets_won': s_won, 'sets_lost': s_lost, 'match_history': disp_hist}

        return {'name': player_name, 'regular': format_bucket(raw_data['regular']), 'fillin': format_bucket(raw_data['fillin']), 'combined': format_bucket(raw_data['combined'])}

    def get_head_to_head(self, p1, p2):
        if p1 not in self.all_players: return None
        history = self.all_players[p1]['combined']['history']
        matches = [m for m in history if m['opponent'].lower() == p2.lower()]
        matches.reverse()
        p1_wins = sum(1 for m in matches if m['result'] == "Win")
        return {'player1': p1, 'player2': p2, 'matches': matches, 'p1_wins': p1_wins, 'p2_wins': len(matches) - p1_wins}