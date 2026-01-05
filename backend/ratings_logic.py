import math

# --- CONFIGURATION ---
TAU = 0.5           
GLICKO_SCALE = 173.7178
EPSILON = 0.000001  

# Defaults for New Players
DEFAULT_RATING = 1000.0
DEFAULT_RD = 350.0
DEFAULT_VOL = 0.06

def _g(phi):
    """Internal Glicko helper"""
    return 1.0 / math.sqrt(1.0 + 3.0 * (phi ** 2) / (math.pi ** 2))

def _E(mu, mu_j, phi_j):
    """Internal Expectation helper"""
    # Safety against overflow in exp()
    val = -_g(phi_j) * (mu - mu_j)
    if val > 100: return 0.0 # exp(100) is huge, 1/(1+huge) is 0
    if val < -100: return 1.0 # exp(-100) is 0, 1/(1+0) is 1
    
    return 1.0 / (1.0 + math.exp(val))

def _update_volatility(vol, delta, phi, v, tau):
    """
    Solves the Glicko-2 equation for new volatility.
    Includes DivisionByZero protection.
    """
    # Safety Check: Prevent log(0) error
    if vol <= 0.0001: vol = 0.06 
        
    a = math.log(vol ** 2)
    
    def f(x):
        # Optimization: Limit exponential growth
        if x > 50: x = 50 
        
        term1 = (math.exp(x) * (delta ** 2 - phi ** 2 - v - math.exp(x))) / \
                (2 * ((phi ** 2 + v + math.exp(x)) ** 2))
        term2 = (x - a) / (tau ** 2)
        return term1 - term2

    A = a
    if (delta ** 2) > (phi ** 2 + v):
        B = math.log(delta ** 2 - phi ** 2 - v)
    else:
        k = 1
        while f(a - k * tau) < 0:
            k += 1
        B = a - k * tau

    fA = f(A)
    fB = f(B)

    # Newton-Raphson iteration
    for _ in range(50): # Limit iterations to prevent infinite loops
        if abs(B - A) <= EPSILON: break
        
        denom = fB - fA
        if abs(denom) < 1e-9: # Prevent division by zero
            C = A
        else:
            C = A + (A - B) * fA / denom
            
        fC = f(C)
        if fC * fB < 0:
            A = B
            fA = fB
        else:
            fA = fA / 2.0
            
        B = C
        fB = fC
            
    return math.exp(A / 2.0)

def calculate_match(winner_stats, loser_stats, winner_score, loser_score):
    """
    Implements Glicko-2 with safeguards.
    """
    # 1. SETUP DATA
    r_w = float(winner_stats.get('rating', DEFAULT_RATING))
    rd_w = float(winner_stats.get('rd', DEFAULT_RD))
    vol_w = float(winner_stats.get('vol', DEFAULT_VOL))
    
    r_l = float(loser_stats.get('rating', DEFAULT_RATING))
    rd_l = float(loser_stats.get('rd', DEFAULT_RD))
    vol_l = float(loser_stats.get('vol', DEFAULT_VOL))

    # Convert to Glicko-2 Scale
    mu_w = (r_w - 1500) / GLICKO_SCALE
    phi_w = rd_w / GLICKO_SCALE
    
    mu_l = (r_l - 1500) / GLICKO_SCALE
    phi_l = rd_l / GLICKO_SCALE

    # 2. DAMPENING
    dampening = 1.0
    try:
        w_score = int(winner_score)
        l_score = int(loser_score)
        total_sets = w_score + l_score
        if (total_sets == 5 and w_score == 3) or (total_sets == 7 and w_score == 4):
            dampening = 0.6
    except: pass

    # --- WINNER CALC ---
    g_phi_l = _g(phi_l)
    E_w = _E(mu_w, mu_l, phi_l)
    
    # CRITICAL FIX: Clamp E to prevent division by zero in variance calculation
    # If E is 1.0, then (1-E) is 0.0, causing the crash.
    E_w = max(0.0001, min(0.9999, E_w))
    
    v_w = 1.0 / ((g_phi_l ** 2) * E_w * (1 - E_w))
    delta_w = v_w * (g_phi_l * (1.0 - E_w))
    
    new_vol_w = _update_volatility(vol_w, delta_w, phi_w, v_w, TAU)

    phi_star_w = math.sqrt(phi_w ** 2 + new_vol_w ** 2)
    new_phi_w = 1.0 / math.sqrt(1.0 / (phi_star_w ** 2) + 1.0 / v_w)
    new_mu_w = mu_w + (new_phi_w ** 2) * (g_phi_l * (1.0 - E_w)) * dampening

    # --- LOSER CALC ---
    g_phi_w = _g(phi_w)
    E_l = _E(mu_l, mu_w, phi_w)
    
    # CRITICAL FIX: Clamp E here too
    E_l = max(0.0001, min(0.9999, E_l))
    
    v_l = 1.0 / ((g_phi_w ** 2) * E_l * (1 - E_l))
    delta_l = v_l * (g_phi_w * (0.0 - E_l)) 
    
    new_vol_l = _update_volatility(vol_l, delta_l, phi_l, v_l, TAU)
    
    phi_star_l = math.sqrt(phi_l ** 2 + new_vol_l ** 2)
    new_phi_l = 1.0 / math.sqrt(1.0 / (phi_star_l ** 2) + 1.0 / v_l)
    new_mu_l = mu_l + (new_phi_l ** 2) * (g_phi_w * (0.0 - E_l)) * dampening

    # 5. CONVERT BACK
    return {
        'winner': {
            'rating': new_mu_w * GLICKO_SCALE + 1500,
            'rd': new_phi_w * GLICKO_SCALE,
            'vol': new_vol_w
        },
        'loser': {
            'rating': new_mu_l * GLICKO_SCALE + 1500,
            'rd': new_phi_l * GLICKO_SCALE,
            'vol': new_vol_l
        }
    }