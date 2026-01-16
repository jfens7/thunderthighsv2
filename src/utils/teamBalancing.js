// src/utils/teamBalancing.js

/**
 * Parses the raw rating string from the Master Sheet.
 * Format expected: "387.628537 +/- 45.179911"
 * Returns: Integer (e.g., 387)
 */
export const parseRating = (ratingString) => {
  if (!ratingString || typeof ratingString !== 'string') return 0;

  // 1. Split at the first decimal point to ignore precision and deviation
  // "387.628537 +/- ..." -> "387"
  const wholeNumberPart = ratingString.split('.')[0];

  // 2. Convert to integer to ensure math operations work correctly
  const ratingInt = parseInt(wholeNumberPart, 10);

  return isNaN(ratingInt) ? 0 : ratingInt;
};

/**
 * Generates balanced teams based on ratings using a "Snake Draft" pattern.
 * * @param {Array} players - Array of player objects. 
 * Must have 'rating' (raw string) or 'parsedRating' property.
 * @param {Number} teamSize - Number of players per team (e.g., 2 or 3).
 * @returns {Array} - Array of Team objects.
 */
export const generateBalancedTeams = (players, teamSize) => {
  if (!players || players.length === 0) return [];

  // 1. Clean and normalize players with the parsed rating
  const pool = players.map(p => ({
    ...p,
    // Use existing parsed rating or parse it now
    numericRating: typeof p.rating === 'number' ? p.rating : parseRating(p.rating)
  }));

  // 2. Sort players from Highest to Lowest rating
  pool.sort((a, b) => b.numericRating - a.numericRating);

  // 3. Calculate number of teams needed
  // Note: If players don't divide perfectly, the last team will be smaller (or use reserves)
  const numberOfTeams = Math.ceil(pool.length / teamSize);

  // Initialize empty teams
  const teams = Array.from({ length: numberOfTeams }, (_, i) => ({
    id: `team-${i + 1}`,
    name: `Team ${i + 1}`,
    players: [],
    totalRating: 0
  }));

  // 4. Distribute players using Snake Draft to balance strength
  // Round 1: Team 1 -> Team N
  // Round 2: Team N -> Team 1
  // Round 3: Team 1 -> Team N
  
  pool.forEach((player, index) => {
    const roundNumber = Math.floor(index / numberOfTeams);
    const isEvenRound = roundNumber % 2 === 0;

    // Determine which team gets this player
    let teamIndex;
    if (isEvenRound) {
      // Forward: 0, 1, 2...
      teamIndex = index % numberOfTeams;
    } else {
      // Backward: 2, 1, 0...
      teamIndex = (numberOfTeams - 1) - (index % numberOfTeams);
    }

    if (teams[teamIndex]) {
      teams[teamIndex].players.push(player);
      teams[teamIndex].totalRating += player.numericRating;
    }
  });

  return teams;
};