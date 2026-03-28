# backend/glicko.py
import datetime

DEFAULT_RATING = 1000.0
DEFAULT_RD = 300.0
RATING_START_DATE = datetime.date(2025, 12, 25)

def calculate_match(w, l, s1, s2, k_win=1.0, k_loss=1.4, anti_riot=True, point_scalar=1.0):
    total_sets = s1 + s2
    if total_sets == 0: return {'winner': w, 'loser': l}
    
    w_score = 0.7 + 0.3 * ((s1 - s2) / total_sets)
    l_score = 1.0 - w_score
    
    E_w = 1.0 / (1.0 + 10.0 ** ((l['rating'] - w['rating']) / 400.0))
    E_l = 1.0 - E_w
    
    K_w = max(30.0, w['rd'] * k_win) * point_scalar
    K_l = max(40.0, l['rd'] * k_loss) * point_scalar
    
    w_shift = K_w * (w_score - E_w)
    l_shift = K_l * (l_score - E_l)
    
    w_rd_shift = -4.0
    l_rd_shift = -4.0
    
    if anti_riot:
        if w_shift < 0: w_shift = 0.0; w_rd_shift = 5.0
        if l_shift > 0: l_shift = 0.0; l_rd_shift = 2.0
        
    w['rating'] += w_shift
    l['rating'] += l_shift
    w['rd'] = max(20.0, min(350.0, w['rd'] + w_rd_shift))
    l['rd'] = max(20.0, min(350.0, l['rd'] + l_rd_shift))
    
    return {'winner': w, 'loser': l}

class RatingEngine:
    def __init__(self): 
        self.players = {}
    
    def get_rating(self, name):
        if name not in self.players: self.players[name] = {'rating': DEFAULT_RATING, 'rd': DEFAULT_RD, 'vol': 0.06}
        return self.players[name]
        
    def set_seed(self, name, rating, rd=None, vol=None):
        try:
            r_val = float(rating); rd_val = float(rd) if rd and str(rd).strip() else DEFAULT_RD
            if rd_val < 0: rd_val = DEFAULT_RD
            self.players[name] = {'rating': r_val, 'rd': rd_val, 'vol': float(vol) if vol is not None else 0.06}
        except ValueError: pass
        
    def update_match(self, p1_name, p2_name, s1, s2, game_history='', k_win=1.0, k_loss=1.4, anti_riot=True):
        p1_stats = self.get_rating(p1_name); p2_stats = self.get_rating(p2_name)
        r1_old = p1_stats['rating']; rd1_old = p1_stats['rd']; r2_old = p2_stats['rating']; rd2_old = p2_stats['rd']
        
        if s1 == s2: return {'p1_delta': 0, 'p2_delta': 0, 'p1_before': r1_old, 'p1_rd_before': rd1_old, 'p1_after': r1_old, 'p1_rd_after': rd1_old, 'p2_before': r2_old, 'p2_rd_before': rd2_old, 'p2_after': r2_old, 'p2_rd_after': rd2_old}
        
        point_scalar = 1.0
        if game_history:
            try:
                p1_pts = 0; p2_pts = 0
                for g in str(game_history).split(','):
                    pts = g.strip().split('-')
                    if len(pts) == 2:
                        p1_pts += int(pts[0])
                        p2_pts += int(pts[1])
                
                if p1_pts > 0 and p2_pts > 0:
                    ratio = (p1_pts / p2_pts) if s1 > s2 else (p2_pts / p1_pts)
                    if ratio >= 2.0: point_scalar = 1.25
                    elif ratio >= 1.5: point_scalar = 1.10
                    elif ratio < 1.15: point_scalar = 0.85
            except: pass

        if s1 > s2:
            res = calculate_match(p1_stats, p2_stats, s1, s2, k_win, k_loss, anti_riot, point_scalar)
            self.players[p1_name] = res['winner']
            self.players[p2_name] = res['loser']
        else:
            res = calculate_match(p2_stats, p1_stats, s2, s1, k_win, k_loss, anti_riot, point_scalar)
            self.players[p2_name] = res['winner']
            self.players[p1_name] = res['loser']
            
        return {
            'p1_delta': self.players[p1_name]['rating'] - r1_old, 'p2_delta': self.players[p2_name]['rating'] - r2_old, 
            'p1_before': r1_old, 'p1_rd_before': rd1_old, 'p1_after': self.players[p1_name]['rating'], 'p1_rd_after': self.players[p1_name]['rd'], 
            'p2_before': r2_old, 'p2_rd_before': rd2_old, 'p2_after': self.players[p2_name]['rating'], 'p2_rd_after': self.players[p2_name]['rd']
        }