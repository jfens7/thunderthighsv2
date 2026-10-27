# backend/glicko.py
import datetime
import math
import re

DEFAULT_RATING = 1050.0
DEFAULT_RD = 200.0
MIN_RD = 25.0
TARGET_RD = 35.0
MAX_RD = 400.0
DEFAULT_VOL_SCORE = 25.0
RATING_START_DATE = datetime.date(2022, 1, 1)

# Division Brackets: (Floor, Ceiling, Default Starting Seed)
# Note: 0 represents "All Stars" (0 to 785)
DIVISION_BRACKETS = {
    1: (1201.0, 9999.0, 1350.0),
    2: (901.0, 1200.0, 1050.0),
    3: (601.0, 900.0, 750.0),
    4: (301.0, 600.0, 450.0),
    5: (1.0, 300.0, 150.0),
    0: (0.0, 785.0, 450.0)
}

DIVISION_LABELS = {
    1: "Division 1 (1201–9999)",
    2: "Division 2 (901–1200)",
    3: "Division 3 (601–900)",
    4: "Division 4 (301–600)",
    5: "Division 5 (1–300)",
    0: "All Stars (0–785)"
}

DIV_STRENGTH_ORDER = {
    1: 60,
    2: 50,
    3: 40,
    0: 35,
    4: 20,
    5: 10
}

VOLATILITY_GUIDE = [
    {"min": 1, "max": 25, "tier": "The Metronome", "label": "Low Volatility", "color": "emerald", "description": "Rock-solid consistency."},
    {"min": 26, "max": 55, "tier": "Standard Form", "label": "Normal Volatility", "color": "blue", "description": "Typical competitive club form."},
    {"min": 56, "max": 80, "tier": "Wildcard", "label": "High Volatility", "color": "amber", "description": "Streaky and dangerous."},
    {"min": 81, "max": 100, "tier": "Pure Chaos", "label": "Extreme Volatility", "color": "rose", "description": "Completely immune to expectations."}
]


def get_volatility_tier(vol_score):
    v = float(vol_score or DEFAULT_VOL_SCORE)
    for item in VOLATILITY_GUIDE:
        if v <= item["max"]:
            return item
    return VOLATILITY_GUIDE[-1]


def parse_division_number(div_str):
    if div_str is None:
        return None
    if isinstance(div_str, int) and div_str in DIVISION_BRACKETS:
        return div_str
    s = str(div_str).strip().lower()
    if not s:
        return None
    if "all star" in s or "all-star" in s or "allstar" in s:
        return 0
    if "premier" in s or "open" in s or "super" in s:
        return 1
    m = re.search(r'([1-5])', s)
    if m:
        return int(m.group(1))
    return None


def clamp_public_rating(true_rating, div_num):
    r = max(0.0, float(true_rating))
    if div_num is None or div_num not in DIVISION_BRACKETS:
        return max(1.0, r)
    floor_val, ceil_val, _ = DIVISION_BRACKETS[div_num]
    return max(floor_val, min(ceil_val, r))


def compute_next_rd(current_rd, matches_played_before, anti_riot_bump=0.0):
    if matches_played_before == 0:
        return 130.0
    elif matches_played_before == 1:
        return 80.0
    elif matches_played_before == 2:
        return 60.0

    if anti_riot_bump > 0:
        return max(MIN_RD, min(MAX_RD, current_rd + anti_riot_bump))

    if current_rd > TARGET_RD:
        drop = max(1.5, (current_rd - 30.0) * 0.28)
        new_rd = current_rd - drop
    else:
        drop = max(0.35, (current_rd - MIN_RD) * 0.12)
        new_rd = current_rd - drop

    return max(MIN_RD, min(MAX_RD, new_rd))


def compute_match_volatility(expected_win_prob, actual_set_share, current_vol, matches_played):
    surprise = abs(actual_set_share - expected_win_prob)
    instant_chaos = min(100.0, max(5.0, (surprise / 0.52) * 85.0))
    alpha = 0.28 if matches_played < 8 else 0.18
    new_vol = (current_vol * (1.0 - alpha)) + (instant_chaos * alpha)
    return max(1.0, min(100.0, round(new_vol, 1)))


def calculate_match(w, l, s1, s2, k_win=1.0, k_loss=1.4, anti_riot=True, point_scalar=1.0):
    total_sets = s1 + s2
    if total_sets == 0:
        return {'winner': w, 'loser': l}

    w_score = 0.7 + 0.3 * ((s1 - s2) / total_sets)
    l_score = 1.0 - w_score

    w_true = max(1.0, float(w.get('true_rating', w.get('rating', DEFAULT_RATING))))
    l_true = max(1.0, float(l.get('true_rating', l.get('rating', DEFAULT_RATING))))

    E_w = 1.0 / (1.0 + 10.0 ** ((l_true - w_true) / 400.0))
    E_l = 1.0 - E_w

    w_rd = float(w.get('rd', DEFAULT_RD))
    l_rd = float(l.get('rd', DEFAULT_RD))

    K_w = max(24.0, w_rd * 0.85 * k_win) * point_scalar
    K_l = max(28.0, l_rd * 0.95 * k_loss) * point_scalar

    w_shift = K_w * (w_score - E_w)
    l_shift = K_l * (l_score - E_l)

    w_bump = 0.0
    l_bump = 0.0

    if anti_riot:
        if w_shift < 0:
            w_shift = 0.0
            w_bump = 3.0
        if l_shift > 0:
            l_shift = 0.0
            l_bump = 1.5

    w_matches = int(w.get('matches_played', 0))
    l_matches = int(l.get('matches_played', 0))

    w_set_share = s1 / float(total_sets)
    l_set_share = s2 / float(total_sets)
    w['vol'] = compute_match_volatility(E_w, w_set_share, float(w.get('vol', DEFAULT_VOL_SCORE)), w_matches)
    l['vol'] = compute_match_volatility(E_l, l_set_share, float(l.get('vol', DEFAULT_VOL_SCORE)), l_matches)

    w['true_rating'] = max(1.0, w_true + w_shift)
    l['true_rating'] = max(1.0, l_true + l_shift)

    w['public_rating'] = clamp_public_rating(w['true_rating'], w.get('home_div'))
    l['public_rating'] = clamp_public_rating(l['true_rating'], l.get('home_div'))
    w['rating'] = w['public_rating']
    l['rating'] = l['public_rating']

    w['rd'] = compute_next_rd(w_rd, w_matches, w_bump)
    l['rd'] = compute_next_rd(l_rd, l_matches, l_bump)

    w['matches_played'] = w_matches + 1
    l['matches_played'] = l_matches + 1

    return {'winner': w, 'loser': l}


class RatingEngine:
    def __init__(self):
        self.players = {}

    def get_rating(self, name, div_str=None):
        div_num = parse_division_number(div_str)
        if name not in self.players:
            if div_num is not None and div_num in DIVISION_BRACKETS:
                seed_rating = DIVISION_BRACKETS[div_num][2]
            else:
                seed_rating = DEFAULT_RATING
            pub_rating = clamp_public_rating(seed_rating, div_num)
            self.players[name] = {
                'rating': pub_rating,
                'public_rating': pub_rating,
                'true_rating': seed_rating,
                'rd': DEFAULT_RD,
                'vol': DEFAULT_VOL_SCORE,
                'home_div': div_num,
                'season_divs': {},
                'active_season': None,
                'matches_played': 0
            }
        else:
            # Enforce strict division cap clamping even on existing memory lookup
            p = self.players[name]
            if div_num is not None:
                if p.get('home_div') is None:
                    p['home_div'] = div_num
                # If player is playing in a specific division match, ensure their public rating respects that active division ceiling!
                p['public_rating'] = clamp_public_rating(p['true_rating'], div_num)
                p['rating'] = p['public_rating']
        return self.players[name]

    def set_seed(self, name, rating, rd=None, vol=None, div_str=None):
        try:
            r_val = max(1.0, float(rating))
            rd_val = float(rd) if rd is not None and str(rd).strip() != '' else DEFAULT_RD
            if rd_val < MIN_RD or rd_val > MAX_RD:
                rd_val = DEFAULT_RD
            vol_val = float(vol) if vol is not None and str(vol).strip() != '' else DEFAULT_VOL_SCORE
            if vol_val < 1.0:
                vol_val = DEFAULT_VOL_SCORE

            existing = self.players.get(name, {})
            parsed_div = parse_division_number(div_str)
            div_num = parsed_div if parsed_div is not None else existing.get('home_div')
            pub_val = clamp_public_rating(r_val, div_num)

            self.players[name] = {
                'rating': pub_val,
                'public_rating': pub_val,
                'true_rating': r_val,
                'rd': rd_val,
                'vol': vol_val,
                'home_div': div_num,
                'season_divs': existing.get('season_divs', {}),
                'active_season': existing.get('active_season'),
                'matches_played': existing.get('matches_played', 3)
            }
        except ValueError:
            pass

    def update_player_division(self, name, div_str, is_fillin=False, season_str=None):
        stats = self.get_rating(name, div_str)
        if is_fillin:
            return None

        new_div = parse_division_number(div_str)
        if new_div is None or new_div not in DIVISION_BRACKETS:
            return None

        s_key = str(season_str or 'Current').strip()
        season_divs = stats.setdefault('season_divs', {})
        divs_in_season = season_divs.setdefault(s_key, set())
        divs_in_season.add(new_div)
        stats['active_season'] = s_key

        effective_new_div = max(divs_in_season, key=lambda d: DIV_STRENGTH_ORDER.get(d, 0))

        old_div = stats.get('home_div')
        if old_div is None:
            stats['home_div'] = effective_new_div
            if stats.get('matches_played', 0) == 0 and stats['true_rating'] == DEFAULT_RATING:
                stats['true_rating'] = DIVISION_BRACKETS[effective_new_div][2]
            stats['public_rating'] = clamp_public_rating(stats['true_rating'], effective_new_div)
            stats['rating'] = stats['public_rating']
            return None

        if effective_new_div == old_div:
            stats['public_rating'] = clamp_public_rating(stats['true_rating'], effective_new_div)
            stats['rating'] = stats['public_rating']
            return None

        old_pub = stats['public_rating']
        old_true = stats['true_rating']
        old_rd = stats['rd']
        old_strength = DIV_STRENGTH_ORDER.get(old_div, 0)
        new_strength = DIV_STRENGTH_ORDER.get(effective_new_div, 0)

        if new_strength < old_strength:
            event_type = "DEMOTION"
            stats['rd'] = min(MAX_RD, max(stats['rd'] + 30.0, 75.0))
        else:
            event_type = "PROMOTION"
            stats['rd'] = min(MAX_RD, max(stats['rd'] + 15.0, 55.0))

        stats['home_div'] = effective_new_div
        stats['public_rating'] = clamp_public_rating(stats['true_rating'], effective_new_div)
        stats['rating'] = stats['public_rating']

        return {
            'type': event_type,
            'old_div': old_div,
            'new_div': effective_new_div,
            'old_pub': old_pub,
            'new_pub': stats['public_rating'],
            'old_true': old_true,
            'new_true': stats['true_rating'],
            'old_rd': old_rd,
            'new_rd': stats['rd']
        }

    def update_match(self, p1_name, p2_name, s1, s2, game_history='', k_win=1.0, k_loss=1.4, anti_riot=True, div_str=None, p1_fill=False, p2_fill=False, season_str=None):
        if div_str:
            self.update_player_division(p1_name, div_str, is_fillin=p1_fill, season_str=season_str)
            self.update_player_division(p2_name, div_str, is_fillin=p2_fill, season_str=season_str)

        p1_stats = self.get_rating(p1_name, div_str)
        p2_stats = self.get_rating(p2_name, div_str)

        pub1_old = p1_stats['public_rating']
        true1_old = p1_stats['true_rating']
        rd1_old = p1_stats['rd']
        vol1_old = p1_stats.get('vol', DEFAULT_VOL_SCORE)

        pub2_old = p2_stats['public_rating']
        true2_old = p2_stats['true_rating']
        rd2_old = p2_stats['rd']
        vol2_old = p2_stats.get('vol', DEFAULT_VOL_SCORE)

        if s1 == s2:
            return {
                'p1_delta': 0.0, 'p2_delta': 0.0,
                'p1_true_delta': 0.0, 'p2_true_delta': 0.0,
                'p1_before': pub1_old, 'p1_rd_before': rd1_old, 'p1_after': pub1_old, 'p1_rd_after': rd1_old,
                'p2_before': pub2_old, 'p2_rd_before': rd2_old, 'p2_after': pub2_old, 'p2_rd_after': rd2_old,
                'p1_true_before': true1_old, 'p1_true_after': true1_old,
                'p2_true_before': true2_old, 'p2_true_after': true2_old,
                'p1_vol_before': vol1_old, 'p1_vol_after': vol1_old,
                'p2_vol_before': vol2_old, 'p2_vol_after': vol2_old
            }

        point_scalar = 1.0
        if game_history:
            try:
                p1_pts = 0
                p2_pts = 0
                for g in str(game_history).split(','):
                    pts = g.strip().split('-')
                    if len(pts) == 2:
                        p1_pts += int(pts[0])
                        p2_pts += int(pts[1])

                if p1_pts > 0 and p2_pts > 0:
                    ratio = (p1_pts / p2_pts) if s1 > s2 else (p2_pts / p1_pts)
                    if ratio >= 2.0:
                        point_scalar = 1.25
                    elif ratio >= 1.5:
                        point_scalar = 1.10
                    elif ratio < 1.15:
                        point_scalar = 0.85
            except Exception:
                pass

        if s1 > s2:
            res = calculate_match(p1_stats, p2_stats, s1, s2, k_win, k_loss, anti_riot, point_scalar)
            self.players[p1_name] = res['winner']
            self.players[p2_name] = res['loser']
        else:
            res = calculate_match(p2_stats, p1_stats, s2, s1, k_win, k_loss, anti_riot, point_scalar)
            self.players[p2_name] = res['winner']
            self.players[p1_name] = res['loser']

        p1_now = self.players[p1_name]
        p2_now = self.players[p2_name]

        return {
            'p1_delta': p1_now['public_rating'] - pub1_old,
            'p2_delta': p2_now['public_rating'] - pub2_old,
            'p1_true_delta': p1_now['true_rating'] - true1_old,
            'p2_true_delta': p2_now['true_rating'] - true2_old,
            'p1_before': pub1_old,
            'p1_rd_before': rd1_old,
            'p1_after': p1_now['public_rating'],
            'p1_rd_after': p1_now['rd'],
            'p2_before': pub2_old,
            'p2_rd_before': rd2_old,
            'p2_after': p2_now['public_rating'],
            'p2_rd_after': p2_now['rd'],
            'p1_true_before': true1_old,
            'p1_true_after': p1_now['true_rating'],
            'p2_true_before': true2_old,
            'p2_true_after': p2_now['true_rating'],
            'p1_vol_before': vol1_old,
            'p1_vol_after': p1_now['vol'],
            'p2_vol_before': vol2_old,
            'p2_vol_after': p2_now['vol']
        }