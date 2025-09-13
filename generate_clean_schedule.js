#!/usr/bin/env node
/**
 * Generate clean regular season schedule files from database
 */

const { Pool } = require('pg');
const fs = require('fs');

// Database connection
const pool = new Pool({
  host: '192.168.1.23',
  database: 'football_tracker',
  user: 'postgres',
  password: 'korn5676',
  port: 5432,
});

async function generateCleanRegularSeason() {
  try {
    console.log('📊 Generating clean regular season schedule files from database...');
    
    // Get all regular season games for 2025, ordered by week and date
    const result = await pool.query(`
      SELECT 
        g.game_id,
        ht.abbreviation as home_team,
        at.abbreviation as away_team,
        g.date,
        g.week
      FROM games g
      JOIN teams ht ON g.home_team_id = ht.id
      JOIN teams at ON g.away_team_id = at.id
      WHERE g.season = 2025 
      AND g.season_type = 'regular'
      ORDER BY g.week, g.date
    `);
    
    const games = result.rows;
    
    if (games.length === 0) {
      console.log('❌ No regular season games found in database');
      return;
    }
    
    // Group games by week
    const weeklyGames = {};
    
    games.forEach(game => {
      const week = game.week;
      if (!weeklyGames[week]) {
        weeklyGames[week] = [];
      }
      
      weeklyGames[week].push({
        game_id: game.game_id,
        home_team: game.home_team,
        away_team: game.away_team,
        date: game.date.toISOString().split('T')[0], // YYYY-MM-DD format
        week: game.week,
        matchup: `${game.away_team} @ ${game.home_team}`,
        url: `https://www.espn.com/nfl/game/_/gameId/${game.game_id}`
      });
    });
    
    console.log(`📊 Found ${games.length} games across ${Object.keys(weeklyGames).length} weeks`);
    
    // Generate files for each week
    const weeks = Object.keys(weeklyGames).map(Number).sort((a, b) => a - b);
    
    for (const week of weeks) {
      const weekGames = weeklyGames[week];
      
      // Generate .txt file
      const txtFilename = `regular_week${week}_2025.txt`;
      let txtContent = `# NFL Regular Week ${week}, 2025 Game URLs\n`;
      txtContent += `# Format: One ESPN game URL per line\n`;
      txtContent += `# Use: python process_nfl_game_file.py ${txtFilename}\n`;
      txtContent += `\n`;
      txtContent += `# ${weekGames.length} games scheduled for this week\n`;
      
      weekGames.forEach(game => {
        txtContent += `# ${game.matchup} - ${game.date}\n`;
        txtContent += `${game.url}\n`;
      });
      
      fs.writeFileSync(txtFilename, txtContent);
      console.log(`✅ Created ${txtFilename} with ${weekGames.length} games`);
      
      // Generate summary JSON
      const jsonFilename = `regular_week${week}_2025_summary.json`;
      const summary = {
        season_type: 'regular',
        week: week,
        year: 2025,
        total_games: weekGames.length,
        generated_date: new Date().toISOString(),
        games: weekGames.map(game => ({
          url: game.url,
          game_id: game.game_id,
          date: game.date + 'T17:00:00Z', // Add time format
          week: game.week,
          season_type: 2, // Regular season
          away_team: game.away_team,
          home_team: game.home_team,
          matchup: game.matchup
        }))
      };
      
      fs.writeFileSync(jsonFilename, JSON.stringify(summary, null, 2));
      console.log(`✅ Created ${jsonFilename}`);
    }
    
    console.log(`\n🎯 Regular season schedule generation complete!`);
    console.log(`📄 Generated ${weeks.length} weekly files`);
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await pool.end();
  }
}

// Run if called directly
if (require.main === module) {
  generateCleanRegularSeason();
}