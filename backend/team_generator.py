def TeamGenerator(players_list, requests, team_size=2):
    """
    Logic engine that pairs High seeds with Low seeds to create balanced teams.
    
    Args:
        players_list (list): List of dicts, e.g., [{'name': 'John', 'id': 1, 'rating': 1200}]
        requests (list): List of strings, e.g., ["Player A / Player B"]
        team_size (int): Number of players per team (default 2)
    """
    
    # SAFETY: specific check to prevent "NoneType" errors if empty data is passed
    if not players_list:
        return {"teams": [], "message": "No players provided"}
    if requests is None:
        requests = []

    final_teams = []
    assigned_ids = set()

    # --- 1. PROCESS REQUESTS (Scanning for "Name / Name" strings) ---
    for req_string in requests:
        # Split string by '/' and clean whitespace
        parts = [n.strip() for n in req_string.split('/') if n.strip()]
        found_players = []
        
        for name in parts:
            # Case-insensitive lookup
            # We look for the first player matching the name who hasn't been assigned yet
            p = next((x for x in players_list if x['name'].lower() == name.lower()), None)
            
            if p and p['id'] not in assigned_ids:
                found_players.append(p)
        
        # Only create the team if we found valid players
        if len(found_players) > 0:
            for p in found_players: 
                assigned_ids.add(p['id'])
            
            # Mark as locked so we know it was a manual request
            final_teams.append({'players': found_players, 'locked': True})

    # --- 2. PREPARE POOL ---
    # Filter out players who are already in requested teams
    pool = [p for p in players_list if p['id'] not in assigned_ids]
    
    # Sort High to Low (Highest rating at index 0)
    # Ensure rating is treated as a float/int
    pool.sort(key=lambda x: float(x['rating']), reverse=True)

    # --- 3. SNAKE DRAFT (High-Low Pairing) ---
    while len(pool) >= team_size:
        new_team = []
        
        # 1. Take the Best available
        new_team.append(pool.pop(0))  
        
        # 2. Take the Worst available (if team size allows)
        if len(new_team) < team_size:
            new_team.append(pool.pop(-1)) 
        
        # 3. If 3+ person teams, fill the middle spots
        while len(new_team) < team_size and pool:
            mid_index = len(pool) // 2
            new_team.append(pool.pop(mid_index))
            
        final_teams.append({'players': new_team, 'locked': False})

    # --- 4. HANDLE LEFTOVERS ---
    # If we have spare players (e.g. odd number of people), add them to existing teams
    # We add them to the last generated teams first (usually the lower rated ones need help)
    for team in reversed(final_teams):
        if not pool:
            break
        if len(team['players']) < team_size + 1: # Allow max 1 extra player per team
             team['players'].append(pool.pop(0))

    # --- 5. FORMAT FOR FRONTEND ---
    formatted = []
    for idx, team in enumerate(final_teams):
        ratings = [float(p['rating']) for p in team['players']]
        # Avoid division by zero
        avg = round(sum(ratings) / len(ratings), 1) if ratings else 0
        
        formatted.append({
            "id": idx + 1,
            "avg": avg,
            "locked": team.get('locked', False),
            "p1": team['players'][0] if len(team['players']) > 0 else None,
            "p2": team['players'][1] if len(team['players']) > 1 else None,
            "p3": team['players'][2] if len(team['players']) > 2 else None,
            # Add p4 support just in case of leftovers
            "p4": team['players'][3] if len(team['players']) > 3 else None 
        })
        
    return {"teams": formatted}


# ==========================================
# TEST BLOCK (Run this file to verify logic)
# ==========================================
if __name__ == "__main__":
    # Mock Data mimicking your Google Sheet
    mock_players = [
        {'id': 101, 'name': 'Geoff Badham', 'rating': 387},
        {'id': 102, 'name': 'Anthony Baguley', 'rating': 1023},
        {'id': 103, 'name': 'Martin Beckwith', 'rating': 939},
        {'id': 104, 'name': 'Rik Bland', 'rating': 998},
        {'id': 105, 'name': 'Don Norris', 'rating': 1050}, # High seed
        {'id': 106, 'name': 'Jane Lee', 'rating': 400},    # Low seed
    ]

    # Mock Requests
    mock_requests = [
        "Don Norris / Anthony Baguley / Jane Lee" # A 3-person request
    ]

    print("Running Generator Test...")
    result = TeamGenerator(mock_players, mock_requests, team_size=2)
    
    import json
    print(json.dumps(result, indent=2))