"""
NFL Team Abbreviation Standardization Module
Ensures consistent team abbreviations across all scrapers and loaders
"""

# CANONICAL team abbreviations - these are the ONLY ones we use
CANONICAL_ABBREVIATIONS = {
    "ARI": "Arizona Cardinals",
    "ATL": "Atlanta Falcons", 
    "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",
    "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",
    "DET": "Detroit Lions",
    "GB": "Green Bay Packers",
    "HOU": "Houston Texans",
    "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs",
    "LAC": "Los Angeles Chargers",
    "LAR": "Los Angeles Rams",
    "LV": "Las Vegas Raiders",
    "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings",
    "NE": "New England Patriots",
    "NO": "New Orleans Saints",
    "NYG": "New York Giants",
    "NYJ": "New York Jets",
    "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers",
    "SF": "San Francisco 49ers",
    "SEA": "Seattle Seahawks",
    "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",
    "WSH": "Washington Commanders"  # Using WSH as canonical
}

# Map ALL variations to canonical abbreviations
ABBREVIATION_MAPPINGS = {
    # Arizona Cardinals
    "ARI": "ARI", "Arizona Cardinals": "ARI", "Cardinals": "ARI", "Arizona": "ARI",
    
    # Atlanta Falcons
    "ATL": "ATL", "Atlanta Falcons": "ATL", "Falcons": "ATL", "Atlanta": "ATL",
    
    # Baltimore Ravens
    "BAL": "BAL", "Baltimore Ravens": "BAL", "Ravens": "BAL", "Baltimore": "BAL",
    
    # Buffalo Bills
    "BUF": "BUF", "Buffalo Bills": "BUF", "Bills": "BUF", "Buffalo": "BUF",
    
    # Carolina Panthers
    "CAR": "CAR", "Carolina Panthers": "CAR", "Panthers": "CAR", "Carolina": "CAR",
    
    # Chicago Bears
    "CHI": "CHI", "Chicago Bears": "CHI", "Bears": "CHI", "Chicago": "CHI",
    
    # Cincinnati Bengals
    "CIN": "CIN", "Cincinnati Bengals": "CIN", "Bengals": "CIN", "Cincinnati": "CIN",
    
    # Cleveland Browns
    "CLE": "CLE", "Cleveland Browns": "CLE", "Browns": "CLE", "Cleveland": "CLE",
    
    # Dallas Cowboys
    "DAL": "DAL", "Dallas Cowboys": "DAL", "Cowboys": "DAL", "Dallas": "DAL",
    
    # Denver Broncos
    "DEN": "DEN", "Denver Broncos": "DEN", "Broncos": "DEN", "Denver": "DEN",
    
    # Detroit Lions
    "DET": "DET", "Detroit Lions": "DET", "Lions": "DET", "Detroit": "DET",
    
    # Green Bay Packers
    "GB": "GB", "Green Bay Packers": "GB", "Packers": "GB", "Green Bay": "GB", "GNB": "GB",
    
    # Houston Texans
    "HOU": "HOU", "Houston Texans": "HOU", "Texans": "HOU", "Houston": "HOU",
    
    # Indianapolis Colts
    "IND": "IND", "Indianapolis Colts": "IND", "Colts": "IND", "Indianapolis": "IND",
    
    # Jacksonville Jaguars
    "JAX": "JAX", "Jacksonville Jaguars": "JAX", "Jaguars": "JAX", "Jacksonville": "JAX", "JAC": "JAX",
    
    # Kansas City Chiefs
    "KC": "KC", "Kansas City Chiefs": "KC", "Chiefs": "KC", "Kansas City": "KC", "KAN": "KC",
    
    # Los Angeles Chargers
    "LAC": "LAC", "Los Angeles Chargers": "LAC", "LA Chargers": "LAC", "Chargers": "LAC", "L.A. Chargers": "LAC",
    
    # Los Angeles Rams  
    "LAR": "LAR", "Los Angeles Rams": "LAR", "LA Rams": "LAR", "Rams": "LAR", "L.A. Rams": "LAR",
    
    # Las Vegas Raiders
    "LV": "LV", "Las Vegas Raiders": "LV", "Raiders": "LV", "Las Vegas": "LV", "LVR": "LV", "OAK": "LV",
    
    # Miami Dolphins
    "MIA": "MIA", "Miami Dolphins": "MIA", "Dolphins": "MIA", "Miami": "MIA",
    
    # Minnesota Vikings
    "MIN": "MIN", "Minnesota Vikings": "MIN", "Vikings": "MIN", "Minnesota": "MIN",
    
    # New England Patriots
    "NE": "NE", "New England Patriots": "NE", "Patriots": "NE", "New England": "NE", "NEP": "NE",
    
    # New Orleans Saints
    "NO": "NO", "New Orleans Saints": "NO", "Saints": "NO", "New Orleans": "NO", "NOR": "NO",
    
    # New York Giants
    "NYG": "NYG", "New York Giants": "NYG", "NY Giants": "NYG", "Giants": "NYG", "N.Y. Giants": "NYG",
    
    # New York Jets
    "NYJ": "NYJ", "New York Jets": "NYJ", "NY Jets": "NYJ", "Jets": "NYJ", "N.Y. Jets": "NYJ",
    
    # Philadelphia Eagles
    "PHI": "PHI", "Philadelphia Eagles": "PHI", "Eagles": "PHI", "Philadelphia": "PHI",
    
    # Pittsburgh Steelers
    "PIT": "PIT", "Pittsburgh Steelers": "PIT", "Steelers": "PIT", "Pittsburgh": "PIT",
    
    # San Francisco 49ers
    "SF": "SF", "San Francisco 49ers": "SF", "49ers": "SF", "San Francisco": "SF", "SFO": "SF",
    
    # Seattle Seahawks
    "SEA": "SEA", "Seattle Seahawks": "SEA", "Seahawks": "SEA", "Seattle": "SEA",
    
    # Tampa Bay Buccaneers
    "TB": "TB", "Tampa Bay Buccaneers": "TB", "Buccaneers": "TB", "Tampa Bay": "TB", "TAM": "TB", "TBB": "TB",
    
    # Tennessee Titans
    "TEN": "TEN", "Tennessee Titans": "TEN", "Titans": "TEN", "Tennessee": "TEN",
    
    # Washington Commanders (IMPORTANT: Using WSH as canonical)
    "WSH": "WSH", "WAS": "WSH", "Washington Commanders": "WSH", "Commanders": "WSH", 
    "Washington": "WSH", "Washington Football Team": "WSH", "Redskins": "WSH"
}

# ESPN team ID mappings
ESPN_TEAM_ID_MAP = {
    '1': 'ATL', '2': 'BUF', '3': 'CHI', '4': 'CIN', '5': 'CLE',
    '6': 'DAL', '7': 'DEN', '8': 'DET', '9': 'GB', '10': 'HOU',
    '11': 'IND', '12': 'KC', '13': 'LV', '14': 'LAC', '15': 'LAR',
    '16': 'MIA', '17': 'MIN', '18': 'NE', '19': 'NO', '20': 'NYG',
    '21': 'NYJ', '22': 'PHI', '23': 'PIT', '24': 'ARI', '25': 'SF',
    '26': 'SEA', '27': 'TB', '28': 'TEN', '29': 'WSH', '30': 'CAR',
    '33': 'BAL', '34': 'JAX'
}

def normalize_team_abbreviation(team_input: str) -> str:
    """
    Normalize any team input to canonical abbreviation
    
    Args:
        team_input: Team name, abbreviation, or variation
        
    Returns:
        Canonical team abbreviation (e.g., 'WSH' for Washington)
    """
    if not team_input:
        return "UNK"
    
    # Clean input
    team_input = str(team_input).strip()
    
    # Check direct mapping
    if team_input in ABBREVIATION_MAPPINGS:
        return ABBREVIATION_MAPPINGS[team_input]
    
    # Check ESPN ID
    if team_input in ESPN_TEAM_ID_MAP:
        return ESPN_TEAM_ID_MAP[team_input]
    
    # Try case-insensitive match
    for key, value in ABBREVIATION_MAPPINGS.items():
        if key.upper() == team_input.upper():
            return value
    
    # Check if it's part of a team name
    team_upper = team_input.upper()
    for key, value in ABBREVIATION_MAPPINGS.items():
        if team_upper in key.upper():
            return value
    
    # Special cases for malformed ESPN data
    if 'orleans' in team_input.lower():
        return 'NO'
    if 'vegas' in team_input.lower():
        return 'LV'
    if 'england' in team_input.lower():
        return 'NE'
    if 'francisco' in team_input.lower():
        return 'SF'
    if 'angeles' in team_input.lower():
        # Need more context - could be LAC or LAR
        if 'chargers' in team_input.lower():
            return 'LAC'
        elif 'rams' in team_input.lower():
            return 'LAR'
    
    return "UNK"

def get_all_canonical_abbreviations():
    """Get list of all canonical team abbreviations"""
    return list(CANONICAL_ABBREVIATIONS.keys())

def get_team_full_name(abbreviation: str) -> str:
    """Get full team name from abbreviation"""
    abbr = normalize_team_abbreviation(abbreviation)
    return CANONICAL_ABBREVIATIONS.get(abbr, "Unknown Team")