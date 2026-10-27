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
# ADVANCED GLICKO-2 RATING ENGINE
# ==========================================
class RatingEngine:
    def __init__(self):
        self.players = {}
        # Glicko-2 Constants (Prof. Mark E. Glickman)
        self.TAU = 0.5  # Volatility constraint (0.5 is standard for highly dynamic tracking)
        self.SCALE = 173.7178
        self.BASE_RATING = 1050.0
        self.EPSILON = 0.000001

    def get_rating(self, player_name, current_match_div=None):
        if player_name not in self.players:
            self.players[player_name] = {
                'rating': self.BASE_RATING,
                'true_rating': self.BASE_RATING,
                'rd': 350.0,
                'vol': 0.06, # Default Glicko-2 Initial Volatility
                'home_div': None
            }
            
        if current_match_div:
            parsed_div = parse_division_number(current_match_div)
            if parsed_div is not None and self.players[player_name]['home_div'] is None:
                self.players[player_name]['home_div'] = parsed_div

        return self.players[player_name]

    def _g(self, phi):
        return 1.0 / math.sqrt(1.0 + 3.0 * (phi ** 2) / (math.pi ** 2))

    def _E(self, mu, mu_j, phi_j):
        return 1.0 / (1.0 + math.exp(-self._g(phi_j) * (mu - mu_j)))

    def update_match(self, p1_name, p2_name, s1, s2, division=None, k_win=1.0, k_loss=1.4, return_payload=True):
        """
        Sophisticated Glicko-2 Implementation.
        Tracks Volatility (sigma) independently of SD (phi) using the Illinois Iteration Method.
        """
        p1 = self.get_rating(p1_name, division)
        p2 = self.get_rating(p2_name, division)

        # Step 1: Convert to Glicko-2 internal scale
        mu1 = (p1['true_rating'] - self.BASE_RATING) / self.SCALE
        phi1 = p1['rd'] / self.SCALE
        sig1 = p1['vol']

        mu2 = (p2['true_rating'] - self.BASE_RATING) / self.SCALE
        phi2 = p2['rd'] / self.SCALE
        sig2 = p2['vol']

        # Determine Outcomes
        if s1 > s2:
            o1, o2 = 1.0, 0.0
        elif s2 > s1:
            o1, o2 = 0.0, 1.0
        else:
            o1, o2 = 0.5, 0.5

        # Step 3: Estimated variance (v)
        g_phi2 = self._g(phi2)
        e1 = self._E(mu1, mu2, phi2)
        v1 = 1.0 / ((g_phi2 ** 2) * e1 * (1.0 - e1)) if e1 < 1 else 9999.0

        g_phi1 = self._g(phi1)
        e2 = self._E(mu2, mu1, phi1)
        v2 = 1.0 / ((g_phi1 ** 2) * e2 * (1.0 - e2)) if e2 < 1 else 9999.0

        # Step 4: Estimated improvement (Delta)
        delta1 = v1 * g_phi2 * (o1 - e1)
        delta2 = v2 * g_phi1 * (o2 - e2)

        # Admin k_win / k_loss multipliers
        delta1 *= (k_win if o1 > o2 else k_loss if o1 < o2 else 1.0)
        delta2 *= (k_win if o2 > o1 else k_loss if o2 < o1 else 1.0)

        # Step 5: Illinois Iterative Algorithm for Volatility
        def compute_new_volatility(sig, phi, v, delta):
            a = math.log(sig ** 2)
            def f(x):
                tmp = phi ** 2 + v + math.exp(x)
                a_part = math.exp(x) * (delta ** 2 - tmp) / (2.0 * (tmp ** 2))
                b_part = (x - a) / (self.TAU ** 2)
                return a_part - b_part
            
            A = a
            if delta ** 2 > phi ** 2 + v:
                B = math.log(delta ** 2 - phi ** 2 - v)
            else:
                k = 1
                while f(a - k * self.TAU) < 0:
                    k += 1
                B = a - k * self.TAU

            fA = f(A)
            fB = f(B)

            while abs(B - A) > self.EPSILON:
                C = A + (A - B) * fA / (fB - fA)
                fC = f(C)
                if fC * fB <= 0:
                    A, fA = B, fB
                else:
                    fA /= 2.0
                B, fB = C, fC
            return math.exp(A / 2.0)

        new_sig1 = compute_new_volatility(sig1, phi1, v1, delta1)
        new_sig2 = compute_new_volatility(sig2, phi2, v2, delta2)

        # Step 6: Update pre-rating period RD
        phi_star1 = math.sqrt(phi1 ** 2 + new_sig1 ** 2)
        phi_star2 = math.sqrt(phi2 ** 2 + new_sig2 ** 2)

        # Step 7: Update true rating and RD
        new_phi1 = 1.0 / math.sqrt(1.0 / (phi_star1 ** 2) + 1.0 / v1)
        new_mu1 = mu1 + (new_phi1 ** 2) * g_phi2 * (o1 - e1)

        new_phi2 = 1.0 / math.sqrt(1.0 / (phi_star2 ** 2) + 1.0 / v2)
        new_mu2 = mu2 + (new_phi2 ** 2) * g_phi1 * (o2 - e2)

        # Step 8: Convert back to 1050-scale
        new_r1 = self.BASE_RATING + self.SCALE * new_mu1
        new_rd1 = self.SCALE * new_phi1

        new_r2 = self.BASE_RATING + self.SCALE * new_mu2
        new_rd2 = self.SCALE * new_phi2

        # Constraints & Hardware Math Limiters
        new_rd1 = max(35.0, min(new_rd1, 350.0))
        new_rd2 = max(35.0, min(new_rd2, 350.0))
        new_r1 = max(0.0, new_r1)  # Absolute floor to prevent negative ratings
        new_r2 = max(0.0, new_r2)

        # Commit to Memory (True Rating)
        self.players[p1_name]['true_rating'] = new_r1
        self.players[p1_name]['rd'] = new_rd1
        self.players[p1_name]['vol'] = new_sig1
        
        self.players[p2_name]['true_rating'] = new_r2
        self.players[p2_name]['rd'] = new_rd2
        self.players[p2_name]['vol'] = new_sig2

        # Enforce Capped Display Mask (Public Rating)
        self.players[p1_name]['rating'] = clamp_public_rating(new_r1, self.players[p1_name].get('home_div'))
        self.players[p2_name]['rating'] = clamp_public_rating(new_r2, self.players[p2_name].get('home_div'))

        if return_payload:
            return {
                "p1_before": p1['rating'],
                "p1_after": self.players[p1_name]['rating'],
                "p1_delta": self.players[p1_name]['rating'] - p1['rating'],
                "p1_true_before": p1['true_rating'],
                "p1_true_after": new_r1,
                "p1_true_delta": new_r1 - p1['true_rating'],
                "p1_rd_before": p1['rd'],
                "p1_rd_after": new_rd1,
                "p2_before": p2['rating'],
                "p2_after": self.players[p2_name]['rating'],
                "p2_delta": self.players[p2_name]['rating'] - p2['rating'],
                "p2_true_before": p2['true_rating'],
                "p2_true_after": new_r2,
                "p2_true_delta": new_r2 - p2['true_rating'],
                "p2_rd_before": p2['rd'],
                "p2_rd_after": new_rd2
            }