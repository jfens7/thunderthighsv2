import math

# ==========================================
# 1. WIN PROBABILITY ENGINE
# ==========================================
def get_win_probability(p1_rating, p2_rating):
    """
    Uses advanced statistical expectation math to calculate the exact percentage 
    chance of one player beating another based on their current ratings.
    """
    # Mathematical formula to determine Expected Value (Probability)
    probability_p1 = 1 / (1 + math.pow(10, (p2_rating - p1_rating) / 400))
    probability_p2 = 1 - probability_p1
    
    return {
        "p1_win_pct": round(probability_p1 * 100, 1),
        "p2_win_pct": round(probability_p2 * 100, 1)
    }

# ==========================================
# 2. ANOMALY / SANDBAGGER DETECTION
# ==========================================
def detect_anomalies(player_name, current_rating, matches_played, win_rate, total_rating_gained):
    """
    An expert system that scans a player's history to flag 'Sandbaggers' 
    (highly skilled players hiding in lower divisions) or inflated ratings.
    """
    flags = []
    risk_level = "Clean"

    if matches_played >= 5: # Need a minimum amount of data to make a guess
        # Flag 1: Winning everything but rating is still low (Classic Sandbagger)
        if win_rate >= 80 and current_rating < 1300:
            flags.append(f"Suspiciously high win rate ({win_rate}%) for current division tier.")
            risk_level = "High Risk"
        
        # Flag 2: Massive sudden spike in skill
        if total_rating_gained > 150:
            flags.append("Massive recent rating spike. Player may have been initially placed in the wrong division.")
            risk_level = "High Risk" if risk_level == "High Risk" else "Moderate"
            
        # Flag 3: Inflated Rating (Losing constantly but still rated high)
        if win_rate <= 20 and current_rating > 1600:
            flags.append("Losing heavily against lower tiers. Rating may be mathematically inflated.")
            risk_level = "Moderate"

    return {
        "player": player_name,
        "risk_level": risk_level,
        "flags": flags,
        "is_flagged": len(flags) > 0
    }

# ==========================================
# 3. PLAYSTYLE MATCHUP ANALYSIS
# ==========================================
def analyze_tactical_matchup(p1_style, p2_style, base_p1_win_pct):
    """
    Adjusts the mathematical win probability by analyzing known table tennis 
    tactics. (e.g., Attackers struggle against heavy Defenders).
    Note: Requires tracking player styles in the future!
    """
    adjusted_pct = base_p1_win_pct
    tactical_notes = []

    # Rule-based adjustments
    if p1_style == "Attacker" and p2_style == "Defender":
        adjusted_pct -= 5.5
        tactical_notes.append("Defenders historically drag out points, which can frustrate aggressive attackers into unforced errors.")
    
    elif p1_style == "Left-Handed" and p2_style == "Right-Handed":
        adjusted_pct += 3.0
        tactical_notes.append("Left-handed players carry a slight statistical advantage due to unfamiliar ball curves on cross-court rallies.")
        
    elif p1_style == "Pips/Junk Rubber" and p2_style == "Standard Rubber":
        adjusted_pct += 4.0
        tactical_notes.append("Pips rubber reverses spin, which statistically causes higher error rates for opponents who aren't used to it.")

    # Keep percentages within reality
    adjusted_pct = max(1.0, min(99.0, adjusted_pct))
    
    return {
        "final_win_pct": round(adjusted_pct, 1),
        "insights": tactical_notes
    }

# ==========================================
# 4. ADVANCED TEAM SYNERGY BALANCING
# ==========================================
def smart_team_balance(pool_of_players):
    """
    Instead of just averaging ratings, this balances teams using a 'Snake Draft' 
    and applies a 'Synergy Penalty' if the skill gap between partners is too large.
    Expects a list of dictionaries: [{'name': 'Jesse', 'rating': 1500}, ...]
    """
    # Sort players from highest rating to lowest
    sorted_players = sorted(pool_of_players, key=lambda x: x['rating'], reverse=True)
    teams = []
    
    # Pair the best player with the worst, second best with second worst, etc.
    while len(sorted_players) >= 2:
        top_player = sorted_players.pop(0)
        bottom_player = sorted_players.pop(-1)
        
        avg_rating = (top_player['rating'] + bottom_player['rating']) / 2
        skill_gap = abs(top_player['rating'] - bottom_player['rating'])
        
        # Synergy Penalty: A 2000 player + an 800 player average 1400.
        # But a 1400 + 1400 team will usually beat them in doubles.
        synergy_penalty = skill_gap * 0.15 
        effective_rating = avg_rating - synergy_penalty
        
        teams.append({
            "player_1": top_player['name'],
            "player_2": bottom_player['name'],
            "base_average": round(avg_rating, 1),
            "effective_team_strength": round(effective_rating, 1),
            "skill_gap_warning": skill_gap > 300
        })
        
    return teams