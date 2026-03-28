# backend/sheets_sync.py
import os
import json
import re
import datetime
import logging
import hashlib
import random
import string
import gspread
from google.oauth2.service_account import Credentials
from firebase_admin import firestore

from backend.glicko import RatingEngine, RATING_START_DATE

logger = logging.getLogger(__name__)
RESULTS_SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po"

class SheetsSyncEngine:
    def __init__(self, core_app):
        self.app = core_app # Reference to ThunderData so we can update its variables
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.sheet_results = None
        self._authenticate()

    def _authenticate(self):
        try:
            creds_json = os.environ.get("GOOGLE_CREDS_JSON")
            if creds_json: 
                self.creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=self.scopes)
            else:
                paths = ["credentials.json", "backend/credentials.json"]
                found = next((p for p in paths if os.path.exists(p)), None)
                self.creds = Credentials.from_service_account_file(found, scopes=self.scopes) if found else None
            
            if self.creds:
                self.client = gspread.authorize(self.creds)
                try: 
                    self.sheet_results = self.client.open_by_key(RESULTS_SPREADSHEET_ID)
                    logger.info("✅ Sync Engine Connected to Master Google Sheet")
                except: 
                    logger.error("❌ Sync Engine failed to open Master Sheet")
        except Exception as e:
            logger.error(f"Sheet Auth Error: {e}")

    def run_sync(self):
        logger.info("⚡️ Sync Engine: Fetching and Processing Data...")
        app = self.app
        app.all_players = {}; app.season_stats = {}; app.seasons_list = ["Career"]
        app.divisions_list = set(); app.weekly_matches = {}; app.rating_engine = RatingEngine(); app.match_history_log = []
        
        if app.db:
            try:
                conf = app.db.collection('system_config').document('main').get().to_dict() or {}
                app.k_win = float(conf.get('k_win_scale', 1.0)); app.k_loss = float(conf.get('k_loss_scale', 1.4))
                admin_docs = list(app.db.collection('admin_users').stream()); active_admins = [d for d in admin_docs if d.to_dict().get('role') in ['admin', 'super_admin', 'temp_super_admin']]
                admin_count = len(active_admins); req_approvals = min(3, admin_count) if admin_count > 0 else 1
                chaos_doc = app.db.collection('system_config').document('chaos_mode').get()
                if chaos_doc.exists:
                    c_data = chaos_doc.to_dict()
                    app.chaos_config = {'weeks': c_data.get('weeks', []), 'approvals': c_data.get('approvals', []), 'req': req_approvals, 'active': len(c_data.get('approvals', [])) >= req_approvals}
                else: app.chaos_config = {'weeks': [], 'approvals': [], 'req': req_approvals, 'active': False}
            except: pass

        app.date_lookup = {}; app.date_to_week_map = {}
        if self.sheet_results: 
            try:
                for row in self.sheet_results.worksheet("Calculated_Dates").get_all_records():
                    s = app._slugify(row.get('Season', '')); d = app._slugify(row.get('Division', '')); w = app._extract_week(row.get('Week', '')); parsed = app._parse_date(str(row.get('Date','')))
                    if parsed: app.date_lookup[f"{s}|{d}|{w}"] = parsed; app.date_to_week_map[f"{s}|{parsed.strftime('%Y-%m-%d')}"] = w
            except: pass
            app.alias_map = {}
            try:
                for row in self.sheet_results.worksheet("Aliases").get_all_records():
                    bad = str(row.get('Bad Name')).strip().lower(); good = str(row.get('Good Name')).strip()
                    if bad and good: app.alias_map[bad] = good
            except: pass
            try:
                ws = self.sheet_results.worksheet("Ratings base"); rows = ws.get_all_values()
                if rows:
                    headers = [str(h).lower().strip() for h in rows[0]]; p_idx = headers.index('player') if 'player' in headers else 0
                    for row in rows[1:]:
                        if len(row) <= p_idx: continue
                        name = app._clean_name(row[p_idx])
                        if name and name not in app.all_players: app.all_players[name] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
            except: pass

        raw_match_queue = []; delta_overrides_dict = {}
        if app.db:
            try:
                for doc in app.db.collection('match_delta_overrides').stream():
                    d = doc.to_dict(); delta_overrides_dict[doc.id] = {'p1_delta': float(d.get('p1_delta', 0)), 'p2_delta': float(d.get('p2_delta', 0))}
            except: pass
            
        if self.sheet_results:
            for worksheet in self.sheet_results.worksheets():
                title = worksheet.title
                if "season" not in title.lower(): continue
                season_name = re.sub(r'(?i)^season\s*:\s*', '', title).strip()
                if season_name not in app.seasons_list: app.seasons_list.append(season_name)
                if season_name not in app.season_stats: app.season_stats[season_name] = {}
                if season_name not in app.weekly_matches: app.weekly_matches[season_name] = {}
                try:
                    all_values = worksheet.get_all_values()
                    if not all_values: continue
                    raw_headers = all_values[0]; clean_headers = []; counts = {'name': 0, 'sets': 0, 'ps': 0, 'player': 0}
                    for h in raw_headers:
                        h_str = str(h).strip(); h_lower = h_str.lower(); key = None
                        if h_lower == 'name': key = 'name'
                        elif h_lower in ['sets', 's']: key = 'sets'
                        elif h_lower in ['pos', 'ps']: key = 'pos'
                        elif h_lower == 'player': key = 'player'
                        if key: counts[key] += 1; clean_headers.append(f"{h_str} {counts[key]}")
                        else: clean_headers.append(h_str)
                    
                    for i, r in enumerate(all_values[1:]):
                        if len(r) < len(clean_headers): r = r + [''] * (len(clean_headers) - len(r))
                        row = {header: r[j] for j, header in enumerate(clean_headers)}
                        if any("doubles" in str(v).lower() for v in row.values()): continue
                        p1 = app._clean_name(app._get_val(row, ['Name 1', 'Player 1', 'Name'])); p2 = app._clean_name(app._get_val(row, ['Name 2', 'Player 2']))
                        if not p1 or not p2: continue
                        div = str(app._get_val(row, ['Division', 'Div'], 'Unknown')).strip(); app.divisions_list.add(div)
                        p1_fill = "S" in str(app._get_val(row, ['PS 1', 'Pos 1', 'Pos'])).upper(); p2_fill = "S" in str(app._get_val(row, ['PS 2', 'Pos 2'])).upper()
                        round_val = app._get_val(row, ['Round', 'Rd', 'Week']); week_num = app._extract_week(round_val) if round_val else "unknown"
                        raw_date = app._get_val(row, ['Date', 'Match Date']); parsed_date = app._parse_date(raw_date)
                        if (not parsed_date) and str(week_num) != "unknown": parsed_date = app.date_lookup.get(f"{app._slugify(season_name)}|{app._slugify(div)}|{week_num}")
                        if not parsed_date: parsed_date = datetime.date(1900, 1, 1)
                        try: s1 = int(app._get_val(row, ['Sets 1', 'S1', 'Sets'])); s2 = int(app._get_val(row, ['Sets 2', 'S2']))
                        except: continue
                        raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_num, 'div': div, 'p1_fill': p1_fill, 'p2_fill': p2_fill, 'game_history': '', 'rich_stats': None, 'manual_override': False, 'sheet_name': worksheet.title, 'row_index': str(i + 2), 'source': 'Spreadsheet'})
                except: pass
                
        if app.db:
            try:
                for doc in app.db.collection('match_results').stream():
                    d = doc.to_dict()
                    if d.get('status') == 'pending' or d.get('status') == 'rejected': continue
                    date_val = d.get('date'); parsed_date = app._parse_date(date_val) or datetime.date.today()
                    raw_season = str(d.get('season', f"Season: {parsed_date.year}")); season_name = re.sub(r'(?i)^season\s*:\s*', '', raw_season).strip()
                    if season_name not in app.seasons_list and season_name != "Unknown": app.seasons_list.append(season_name)
                    if season_name not in app.season_stats: app.season_stats[season_name] = {}
                    if season_name not in app.weekly_matches: app.weekly_matches[season_name] = {}
                    home_p = d.get('home_players', []); away_p = d.get('away_players', [])
                    if not home_p or not away_p: continue
                    p1 = app._clean_name(home_p[0]); p2 = app._clean_name(away_p[0]); s1 = d.get('live_home_sets', 0); s2 = d.get('live_away_sets', 0)
                    if s1 == 0 and s2 == 0 and d.get('game_scores_history'):
                        t1=0; t2=0
                        for s in str(d.get('game_scores_history')).split(','):
                            try: 
                                if int(s.split('-')[0]) > int(s.split('-')[1]): t1+=1
                                else: t2+=1
                            except: pass
                        s1=t1; s2=t2
                    week_val = app._extract_week(d.get('week', 'Unknown'))
                    if str(week_val) == "unknown" and parsed_date: week_val = app.date_to_week_map.get(f"{app._slugify(season_name)}|{parsed_date.strftime('%Y-%m-%d')}", "unknown")
                    rich = d.get('richStats', {}); rich['total_duration'] = d.get('total_duration', '00:00'); rich['play_duration'] = d.get('play_duration', '00:00'); rich['set_scores'] = d.get('set_scores', [])
                    raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_val, 'div': d.get('division', 'Unknown'), 'p1_fill': False, 'p2_fill': False, 'game_history': d.get('game_scores_history', ''), 'rich_stats': rich, 'manual_override': d.get('manual_override', False), 'sheet_name': 'Live Match Data', 'row_index': 'Firebase', 'source': 'Admin/iPad'})
            except: pass

        corrections = {}
        if app.db:
            try:
                for doc in app.db.collection('match_corrections').stream():
                    c = doc.to_dict(); c_p1 = app._clean_name(c.get('p1', '')); c_p2 = app._clean_name(c.get('p2', '')); d_str = str(c.get('date', ''))
                    players = sorted([c_p1, c_p2]); key = f"{d_str}_{players[0]}_{players[1]}"
                    corrections[key] = {'s1': c.get('s1', 0), 's2': c.get('s2', 0), 'c_p1': c_p1, 'new_date': c.get('new_date')}
            except: pass

        for m in raw_match_queue:
            players = sorted([m['p1'], m['p2']]); d_str = m['date'].strftime("%d/%m/%Y") if m['date'] else "nodate"; key = f"{d_str}_{players[0]}_{players[1]}"
            if key in corrections:
                c = corrections[key]
                if m['p1'] == c['c_p1']: m['s1'] = c['s1']; m['s2'] = c['s2']
                else: m['s1'] = c['s2']; m['s2'] = c['s1']
                m['manual_override'] = True
                if c.get('new_date'):
                    parsed_new = app._parse_date(c['new_date'])
                    if parsed_new: m['date'] = parsed_new

        groups = {}
        for m in raw_match_queue:
            players = sorted([m['p1'], m['p2']]); date_key = m['date'].strftime("%Y%m%d") if m['date'] else "nodate"
            match_key = f"{date_key}_{players[0]}_{players[1]}"
            if match_key not in groups: groups[match_key] = []
            groups[match_key].append(m)
        
        cleaned_matches = []
        for key, group in groups.items():
            if len(group) == 1: cleaned_matches.append(group[0]); continue
            firebase_m = [m for m in group if m.get('rich_stats') is not None]; sheet_m = [m for m in group if m.get('rich_stats') is None]; merged = []
            for fm in firebase_m: merged.append(fm); sheet_m.pop(0) if sheet_m else None
            for sm in sheet_m: merged.append(sm)
            cleaned_matches.extend(merged)
        
        cleaned_matches.sort(key=lambda x: x['date']); overrides_dict = {}
        if app.db:
            try:
                for doc in app.db.collection('rating_overrides').stream():
                    d = doc.to_dict(); overrides_dict[d.get('name')] = {'rating': float(d.get('rating', 1500)), 'rd': float(d.get('rd', 75.0)), 'vol': float(d.get('vol', 0.06)), 'date': d.get('date_str', '1900-01-01')}
            except: pass

        player_set = set(); player_overrides_applied = set()
        last_played_dates = {}; DECAY_PER_WEEK = 2.0
        
        for m in cleaned_matches:
            players = sorted([m['p1'], m['p2']]); d_str = m['date'].strftime("%d/%m/%Y") if m['date'] else "nodate"; d_str_fmt = m['date'].strftime("%Y-%m-%d") if m['date'] else "1900-01-01"
            raw_id_string = f"{d_str}_{players[0]}_{players[1]}_{m['s1']}_{m['s2']}"; match_id = hashlib.md5(raw_id_string.encode()).hexdigest()[:6].upper()
            
            if m['date'] > RATING_START_DATE:
                for p in [m['p1'], m['p2']]:
                    if p in last_played_dates:
                        days_inactive = (m['date'] - last_played_dates[p]).days
                        if days_inactive >= 7:
                            weeks_missed = days_inactive // 7
                            p_stats = app.rating_engine.get_rating(p)
                            new_rd = min(350.0, p_stats['rd'] + (weeks_missed * DECAY_PER_WEEK))
                            app.rating_engine.players[p]['rd'] = new_rd
                    last_played_dates[p] = m['date']

            for p in [m['p1'], m['p2']]:
                if p in overrides_dict and p not in player_overrides_applied:
                    if d_str_fmt >= overrides_dict[p]['date']:
                        if p not in app.rating_engine.players: app.rating_engine.get_rating(p)
                        app.rating_engine.players[p]['rating'] = overrides_dict[p]['rating']; app.rating_engine.players[p]['rd'] = overrides_dict[p]['rd']; app.rating_engine.players[p]['vol'] = overrides_dict[p]['vol']
                        player_overrides_applied.add(p)

            deltas = {'p1_delta': 0, 'p2_delta': 0}
            if m['date'] > RATING_START_DATE: 
                if match_id in delta_overrides_dict:
                    p1_d = delta_overrides_dict[match_id]['p1_delta']; p2_d = delta_overrides_dict[match_id]['p2_delta']
                    p1_stats = app.rating_engine.get_rating(m['p1']); p2_stats = app.rating_engine.get_rating(m['p2'])
                    p1_before = p1_stats['rating']; p1_rd_before = p1_stats['rd']; p2_before = p2_stats['rating']; p2_rd_before = p2_stats['rd']
                    app.rating_engine.players[m['p1']]['rating'] += p1_d; app.rating_engine.players[m['p2']]['rating'] += p2_d
                    app.rating_engine.players[m['p1']]['rd'] = max(20.0, app.rating_engine.players[m['p1']]['rd'] - 4.0); app.rating_engine.players[m['p2']]['rd'] = max(20.0, app.rating_engine.players[m['p2']]['rd'] - 4.0)
                    deltas = {'p1_delta': p1_d, 'p2_delta': p2_d, 'p1_before': p1_before, 'p1_rd_before': p1_rd_before, 'p1_after': app.rating_engine.players[m['p1']]['rating'], 'p1_rd_after': app.rating_engine.players[m['p1']]['rd'], 'p2_before': p2_before, 'p2_rd_before': p2_rd_before, 'p2_after': app.rating_engine.players[m['p2']]['rating'], 'p2_rd_after': app.rating_engine.players[m['p2']]['rd']}
                else:
                    m_week = str(m.get('week', '')).lower()
                    is_chaos = app.chaos_config['active'] and m_week in [w.lower() for w in app.chaos_config['weeks']]
                    deltas = app.rating_engine.update_match(m['p1'], m['p2'], m['s1'], m['s2'], m.get('game_history', ''), app.k_win, app.k_loss, not is_chaos)
            
            p1_delta = deltas.get('p1_delta', 0); p2_delta = deltas.get('p2_delta', 0)
            match_hash = f"{d_str}_{m['season']}_{m['week']}_{players[0]}_{players[1]}_{m['s1']}-{m['s2']}"
            record = {'id': match_hash, 'match_id': match_id, 'date': d_str, 'season': m['season'], 'week': m['week'], 'division': m['div'], 'p1': m['p1'], 'p2': m['p2'], 'home_players': [m['p1']], 'away_players': [m['p2']], 's1': m['s1'], 's2': m['s2'], 'score': f"{m['s1']}-{m['s2']}", 'p1_before': deltas.get('p1_before', app.rating_engine.get_rating(m['p1'])['rating']), 'p1_rd_before': deltas.get('p1_rd_before', app.rating_engine.get_rating(m['p1'])['rd']), 'p1_after': deltas.get('p1_after', app.rating_engine.get_rating(m['p1'])['rating']), 'p1_rd_after': deltas.get('p1_rd_after', app.rating_engine.get_rating(m['p1'])['rd']), 'p1_delta': p1_delta, 'p2_before': deltas.get('p2_before', app.rating_engine.get_rating(m['p2'])['rating']), 'p2_rd_before': deltas.get('p2_rd_before', app.rating_engine.get_rating(m['p2'])['rd']), 'p2_after': deltas.get('p2_after', app.rating_engine.get_rating(m['p2'])['rating']), 'p2_rd_after': deltas.get('p2_rd_after', app.rating_engine.get_rating(m['p2'])['rd']), 'p2_delta': p2_delta, 'rich_stats': m.get('rich_stats', {}), 'game_history': m.get('game_history', ''), 'sheet_name': m.get('sheet_name', 'Unknown'), 'row_index': m.get('row_index', '?'), 'source': m.get('source', 'Unknown')}
            app.match_history_log.append(record)
            
            def add_stats(p, sets_for, sets_against, is_p1, opp, fill, delta):
                if p not in app.all_players: app.all_players[p] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
                if m['season'] not in app.season_stats: app.season_stats[m['season']] = {}
                if p not in app.season_stats[m['season']]: app.season_stats[m['season']][p] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
                result = "Win" if sets_for > sets_against else "Loss"
                h_rec = {'season': m['season'], 'week': m['week'], 'date': d_str, 'opponent': opp, 'result': result, 'score': f"{sets_for}-{sets_against}", 'type': 'Fill-in' if fill else 'Regular', 'division': m['div'], 'details': m.get('game_history', ''), 'rich_stats': m.get('rich_stats'), 'match_id': match_id, 'delta': delta, 'sheet_name': m.get('sheet_name', 'Unknown'), 'row_index': m.get('row_index', '?')}
                for s_dict in [app.all_players[p], app.season_stats[m['season']][p]]:
                    buckets = [s_dict['combined'], s_dict['fillin'] if fill else s_dict['regular']]
                    for b in buckets:
                        b['matches'] += 1; b['sets_won'] += sets_for; b['sets_lost'] += sets_against
                        if result == "Win": b['wins'] += 1
                        else: b['losses'] += 1
                        b['history'].append(h_rec)
            add_stats(m['p1'], m['s1'], m['s2'], True, m['p2'], m['p1_fill'], p1_delta); add_stats(m['p2'], m['s2'], m['s1'], False, m['p1'], m['p2_fill'], p2_delta); player_set.add(m['p1']); player_set.add(m['p2'])

            if str(m['week']) != "unknown":
                wk = str(m['week'])
                if wk not in app.weekly_matches[m['season']]: app.weekly_matches[m['season']][wk] = []
                app.weekly_matches[m['season']][wk].append({'p1': m['p1'], 'p2': m['p2'], 'score': f"{m['s1']}-{m['s2']}", 'division': m['div'], 'date': d_str})
        
        try:
            app.player_ids = {}; app.id_to_name = {}; ws = self.sheet_results.worksheet("Players"); all_values = ws.get_all_values()
            headers = [str(h).lower().strip() for h in all_values[0]]; name_col = headers.index("player name") if "player name" in headers else 0; id_col = headers.index("player id") if "player id" in headers else 1; existing_names = {}
            for i, row in enumerate(all_values[1:], start=2): 
                if not row: continue
                p_name = str(row[name_col]).strip() if len(row) > name_col else ""; clean_n = app._clean_name(p_name); p_id = str(row[id_col]).strip() if len(row) > id_col else ""
                if clean_n: app.player_ids[clean_n] = p_id; existing_names[clean_n.lower()] = True
            for p_name in player_set:
                if p_name.lower() not in existing_names: new_id = app._generate_player_id(); ws.append_row([p_name, new_id, datetime.date.today().strftime("%Y-%m-%d"), "Active"]); app.player_ids[p_name] = new_id
            app.id_to_name = {v: k for k, v in app.player_ids.items()}
        except: pass

        if app.db:
            try:
                batch = app.db.batch(); batch_count = 0
                for player_name, stats in app.all_players.items():
                    rat = app.rating_engine.get_rating(player_name); safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
                    batch.set(app.db.collection('player_profiles').document(safe_id), {'name': player_name, 'rating': int(rat['rating']), 'wins': stats['combined']['wins'], 'losses': stats['combined']['losses'], 'matches_played': stats['combined']['matches'], 'last_updated': datetime.datetime.now()}, merge=True)
                    batch_count += 1
                    if batch_count >= 400: batch.commit(); batch = app.db.batch(); batch_count = 0
                if batch_count > 0: batch.commit()
            except: pass