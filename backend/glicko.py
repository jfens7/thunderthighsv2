import math
import re
import datetime

# Fixed: Declared as a proper datetime.date object so sheets_sync.py can compare it mathematically
RATING_START_DATE = datetime.date(2020, 1, 1)

# ==========================================
# DIVISION MAPPINGS & CAPS
# ==========================================
DIVISION_CAPS = {
    1: {"floor": 1300, "ceiling": 9999, "label": "Division 1 (1300+)"},
    2: {"floor": 901,  "ceiling": 1299, "label": "Division 2 (901–1299)"},
    3: {"floor": 601,  "ceiling": 900,  "label": "Division 3 (601–900)"},
    4: {"floor": 301,  "ceiling": 600,  "label": "Division 4 (301–600)"},
    5: {"floor": 0,    "ceiling": 300,  "label": "Division 5 (0–300)"},
    99: {"floor": 0,   "ceiling": 900,  "label": "All Stars (0–900)"}
}

# Fallback dictionary to prevent legacy import errors
DIVISION_LABELS = {k: v["label"] for k, v in DIVISION_CAPS.items()}

VOLATILITY_GUIDE = [
    {"label": "Consistent", "range": "0.05 - 0.059", "desc": "Player performs exactly as expected."},
    {"label": "Normal", "range": "0.06 - 0.069", "desc": "Standard fluctuation in performance."},
    {"label": "Erratic", "range": "0.07+", "desc": "Highly unpredictable match outcomes."}
]

def parse_division_number(div_str):
    if not div_str:
        return None
    ds = str(div_str).lower().strip()
    if 'all stars' in ds:
        return 99
    match = re.search(r'\d+', ds)
    if match:
        return int(match.group())
    return None

def format_division_label(div_num):
    if div_num in DIVISION_CAPS:
        return DIVISION_CAPS[div_num]["label"]
    return "Unassigned Bracket"

def clamp_public_rating(true_rating, home_div):
    # HARD FLOOR: Ensure public rating never drops below 0
    if home_div is None or home_div not in DIVISION_CAPS:
        return max(0.0, float(true_rating))
    
    ceiling = DIVISION_CAPS[home_div]["ceiling"]
    return max(0.0, min(float(true_rating), float(ceiling)))

# ==========================================
# GLICKO RATING ENGINE
# ==========================================
class RatingEngine:
    def __init__(self):
        self.players = {}
        self.q = math.log(10) / 400
        self.pi_sq = math.pi ** 2

    def get_rating(self, player_name, current_match_div=None):
        if player_name not in self.players:
            self.players[player_name] = {
                'rating': 1050.0,
                'true_rating': 1050.0,
                'rd': 350.0,
                'vol': 0.06,
                'home_div': None
            }
            
        if 'true_rating' not in self.players[player_name]:
            self.players[player_name]['true_rating'] = self.players[player_name]['rating']

        if current_match_div:
            parsed_div = parse_division_number(current_match_div)
            if parsed_div is not None and self.players[player_name]['home_div'] is None:
                self.players[player_name]['home_div'] = parsed_div

        return self.players[player_name]

    def _g_rd(self, rd):
        return 1.0 / math.sqrt(1.0 + 3.0 * (self.q ** 2) * (rd ** 2) / self.pi_sq)

    def _e(self, rating, opp_rating, opp_rd):
        return 1.0 / (1.0 + math.pow(10, -self._g_rd(opp_rd) * (rating - opp_rating) / 400.0))

    def _d_sq(self, rating, opp_rating, opp_rd):
        e_val = self._e(rating, opp_rating, opp_rd)
        g_val = self._g_rd(opp_rd)
        return 1.0 / ((self.q ** 2) * (g_val ** 2) * e_val * (1.0 - e_val))

    def update_match(self, p1_name, p2_name, s1, s2, division=None, k_win=1.0, k_loss=1.4, return_payload=True):
        p1 = self.get_rating(p1_name, division)
        p2 = self.get_rating(p2_name, division)

        r1 = p1['true_rating']
        rd1 = p1['rd']
        r2 = p2['true_rating']
        rd2 = p2['rd']

        if s1 > s2:
            outcome_p1 = 1.0
            outcome_p2 = 0.0
            weight = k_win
        elif s2 > s1:
            outcome_p1 = 0.0
            outcome_p2 = 1.0
            weight = k_loss
        else:
            outcome_p1 = 0.5
            outcome_p2 = 0.5
            weight = 1.0

        g_rd2 = self._g_rd(rd2)
        e1 = self._e(r1, r2, rd2)
        d_sq1 = self._d_sq(r1, r2, rd2)
        new_r1 = r1 + (self.q / (1.0 / (rd1 ** 2) + 1.0 / d_sq1)) * g_rd2 * (outcome_p1 - e1) * weight
        new_rd1 = math.sqrt(1.0 / (1.0 / (rd1 ** 2) + 1.0 / d_sq1))

        g_rd1 = self._g_rd(rd1)
        e2 = self._e(r2, r1, rd1)
        d_sq2 = self._d_sq(r2, r1, rd1)
        new_r2 = r2 + (self.q / (1.0 / (rd2 ** 2) + 1.0 / d_sq2)) * g_rd1 * (outcome_p2 - e2) * weight
        new_rd2 = math.sqrt(1.0 / (1.0 / (rd2 ** 2) + 1.0 / d_sq2))

        new_rd1 = max(35.0, min(new_rd1, 350.0))
        new_rd2 = max(35.0, min(new_rd2, 350.0))
        
        # HARD FLOOR TO PREVENT NEGATIVE SPIRALS
        new_r1 = max(0.0, new_r1)
        new_r2 = max(0.0, new_r2)

        self.players[p1_name]['true_rating'] = new_r1
        self.players[p1_name]['rd'] = new_rd1
        self.players[p2_name]['true_rating'] = new_r2
        self.players[p2_name]['rd'] = new_rd2

        self.players[p1_name]['rating'] = clamp_public_rating(new_r1, self.players[p1_name].get('home_div'))
        self.players[p2_name]['rating'] = clamp_public_rating(new_r2, self.players[p2_name].get('home_div'))

        if return_payload:
            return {
                "p1_before": p1['rating'],
                "p1_after": self.players[p1_name]['rating'],
                "p1_delta": self.players[p1_name]['rating'] - p1['rating'],
                "p1_true_before": r1,
                "p1_true_after": new_r1,
                "p1_true_delta": new_r1 - r1,
                "p1_rd_before": rd1,
                "p1_rd_after": new_rd1,
                "p2_before": p2['rating'],
                "p2_after": self.players[p2_name]['rating'],
                "p2_delta": self.players[p2_name]['rating'] - p2['rating'],
                "p2_true_before": r2,
                "p2_true_after": new_r2,
                "p2_true_delta": new_r2 - r2,
                "p2_rd_before": rd2,
                "p2_rd_after": new_rd2
            }