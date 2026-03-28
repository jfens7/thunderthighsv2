# backend/league_engine.py
import re
import random
import datetime
from firebase_admin import firestore
import logging

logger = logging.getLogger(__name__)

class LeagueEngineMixin:
    """Handles Tournaments, Events, PDF Scraping, and Schedules"""

    def admin_create_tournament(self, data, admin_email="Unknown"):
        if not self.db: return False
        try:
            t_id = self._slugify(data.get('name', 'new_tournament')) + f"_{random.randint(100,999)}"
            payload = {'id': t_id, 'name': data.get('name'), 'start_date': data.get('start_date'), 'end_date': data.get('end_date'), 'venue': data.get('venue', 'GCTTA'), 'status': data.get('status', 'Draft'), 'created_by': admin_email, 'timestamp': firestore.SERVER_TIMESTAMP}
            self.db.collection('tournaments').document(t_id).set(payload)
            self._log_audit(admin_email, 'CREATE_TOURNAMENT', f"Created Tournament: {data.get('name')}", {})
            return {"success": True, "tournament_id": t_id}
        except Exception as e: return {"success": False, "error": str(e)}

    def admin_create_event(self, tournament_id, data, admin_email="Unknown"):
        if not self.db: return False
        try:
            event_ref = self.db.collection('tournaments').document(tournament_id).collection('events').document()
            payload = {'id': event_ref.id, 'name': data.get('name'), 'type': data.get('type', 'Singles'), 'max_rating': float(data.get('max_rating')) if data.get('max_rating') else None, 'max_age': int(data.get('max_age')) if data.get('max_age') else None, 'min_age': int(data.get('min_age')) if data.get('min_age') else None, 'time_block': data.get('time_block', 'TBD'), 'price': float(data.get('price', 0.0)), 'max_players': int(data.get('max_players', 64))}
            event_ref.set(payload); self._log_audit(admin_email, 'CREATE_EVENT', f"Added Event {data.get('name')} to {tournament_id}", {})
            return {"success": True, "event_id": event_ref.id}
        except Exception as e: return {"success": False, "error": str(e)}

    def check_eligibility(self, uid, event_data, partner_uid=None):
        if not self.db: return False, ["Database Offline"]
        def _check_player(player_doc, p_label="Player"):
            reasons = []
            if not player_doc.exists: return False, [f"{p_label} profile not found. Please register."]
            p_data = player_doc.to_dict(); max_rating = event_data.get('max_rating')
            if max_rating:
                active_rating = p_data.get('rc_rating', p_data.get('estimated_rating', 1500.0))
                if float(active_rating) >= float(max_rating): reasons.append(f"{p_label} rating ({active_rating}) exceeds the limit of {max_rating}.")
            dob_str = p_data.get('dob')
            if dob_str:
                try:
                    dob = datetime.datetime.strptime(dob_str, "%Y-%m-%d").date(); today = datetime.date.today(); age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                    if event_data.get('max_age') and age > int(event_data.get('max_age')): reasons.append(f"{p_label} is too old ({age} yrs) for this event.")
                    if event_data.get('min_age') and age < int(event_data.get('min_age')): reasons.append(f"{p_label} is too young ({age} yrs) for this event.")
                except: reasons.append(f"Invalid Date of Birth format for {p_label}.")
            else: reasons.append(f"Date of Birth missing for {p_label}.")
            return len(reasons) == 0, reasons

        user_doc = self.db.collection('verified_users').document(uid).get()
        if not user_doc.exists: user_doc = self.db.collection('pending_accounts').document(uid).get()
        user_eligible, user_reasons = _check_player(user_doc, "You")
        if not user_eligible: return False, user_reasons

        if event_data.get('type') == 'Doubles' and partner_uid:
            partner_doc = self.db.collection('verified_users').document(partner_uid).get()
            if not partner_doc.exists: partner_doc = self.db.collection('pending_accounts').document(partner_uid).get()
            partner_eligible, partner_reasons = _check_player(partner_doc, "Your Partner")
            if not partner_eligible: return False, partner_reasons
        return True, ["Eligible"]

    def admin_get_teams(self):
        if not self.db: return []
        try: return [{"id": d.id, **d.to_dict()} for d in self.db.collection('teams').stream()]
        except: return []

    def admin_update_team(self, team_id, players_list, admin_email="Unknown"):
        if not self.db: return False
        try: 
            self.db.collection('teams').document(team_id).set({'players': players_list}, merge=True)
            self._log_audit(admin_email, 'UPDATE_TEAM', f"Updated roster for {team_id}", {}); return True
        except: return False

    def admin_upload_pdf_schedule(self, season, division, file_stream, admin_email="Unknown"):
        if not self.db: return {"success": False, "error": "DB Offline"}
        try:
            import PyPDF2
            pdf_reader = PyPDF2.PdfReader(file_stream)
            raw_text = ""
            for page in pdf_reader.pages: raw_text += page.extract_text() + "\n"

            lines = raw_text.split('\n')
            teams = {}; fixtures = []
            parsing_teams = False; parsing_fixtures = False; current_date = None

            team_pattern = re.compile(r'^(\d+)\s+([A-Z\s]+)\s+([A-Z\s]+)')
            match_pattern = re.compile(r'(\d+)\s*v[s]?\s*(\d+)(?:\s*\(\s*(\d+)\s*\))?', re.IGNORECASE)
            date_pattern = re.compile(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}', re.IGNORECASE)

            for i, line in enumerate(lines):
                line = line.strip()
                if not line: continue
                if "TEAMS" in line.upper(): parsing_teams = True; parsing_fixtures = False; continue
                if "FIXTURE SCHEDULE" in line.upper() or ("DATE" in line.upper() and "MATCH" in line.upper()): parsing_teams = False; parsing_fixtures = True; continue

                if parsing_teams:
                    if re.match(r'^\d+', line):
                        parts = re.split(r'\s{2,}', line)
                        if len(parts) >= 2:
                            team_id = parts[0].strip(); team_name = parts[1].strip(); players = []
                            for p in parts[2:]:
                                cleaned_name = re.sub(r'[\d\-\|\(\)]', '', p).replace(' C ', '').replace(' LEFTY', '').strip()
                                if cleaned_name and len(cleaned_name) > 2: players.append(cleaned_name)
                            teams[team_id] = {"name": team_name, "players": players}

                if parsing_fixtures:
                    date_match = date_pattern.search(line)
                    if date_match:
                        season_year = re.search(r'\d{4}', season)
                        year_str = season_year.group() if season_year else str(datetime.datetime.now().year)
                        current_date = f"{date_match.group()} {year_str}"
                    
                    matches = match_pattern.findall(line)
                    for m in matches:
                        team_home_id = m[0]; team_away_id = m[1]
                        if current_date and team_home_id in teams and team_away_id in teams:
                            fixtures.append({"season": season, "division": division, "date_text": current_date, "home_team": teams[team_home_id]["name"], "away_team": teams[team_away_id]["name"], "home_players": teams[team_home_id]["players"], "away_players": teams[team_away_id]["players"]})

            if len(fixtures) == 0: return {"success": False, "error": "Could not extract any matches. Check PDF format."}

            batch = self.db.batch()
            for fix in fixtures: batch.set(self.db.collection('upcoming_schedule').document(), fix)
            for t_num, t_data in teams.items():
                team_doc_id = f"{self._slugify(season)}_{self._slugify(division)}_{self._slugify(t_data['name'])}"
                batch.set(self.db.collection('teams').document(team_doc_id), {'season': season, 'division': division, 'team_name': t_data['name'], 'players': t_data['players'], 'timestamp': firestore.SERVER_TIMESTAMP}, merge=True)
            batch.commit()
            self._log_audit(admin_email, 'UPLOAD_SCHEDULE', f"Parsed PDF for {season} {division}. Extracted {len(teams)} teams and {len(fixtures)} matchups.", {})
            return {"success": True, "matches_found": len(fixtures), "teams_found": len(teams)}
        except Exception as e: return {"success": False, "error": f"Failed to read PDF: {str(e)}"}

    def admin_get_upcoming_schedules(self):
        if not self.db: return []
        try: return [{"id": d.id, **d.to_dict()} for d in self.db.collection('upcoming_schedule').stream()]
        except: return []

    def admin_delete_upcoming_schedule(self, schedule_id, admin_email="Unknown"):
        if not self.db: return False
        try: self.db.collection('upcoming_schedule').document(schedule_id).delete(); self._log_audit(admin_email, 'DELETE_SCHEDULE', f"Deleted extracted schedule {schedule_id}", {}); return True
        except: return False

    def get_player_upcoming_schedule(self, player_name):
        if not self.db: return []
        try:
            player_clean = self._clean_name(player_name).lower(); matches = []
            for d in self.db.collection('upcoming_schedule').stream():
                m = d.to_dict(); home_p = [self._clean_name(p).lower() for p in m.get('home_players', [])]; away_p = [self._clean_name(p).lower() for p in m.get('away_players', [])]
                is_playing = False
                for hp in home_p:
                    if player_clean in hp or hp in player_clean: is_playing = True
                for ap in away_p:
                    if player_clean in ap or ap in player_clean: is_playing = True
                if is_playing: matches.append(m)
            return matches
        except: return []