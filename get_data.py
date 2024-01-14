from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import players
import pandas as pd
from tqdm import tqdm
from requests.exceptions import ReadTimeout


#obtaining data from nba_api, standard json access and conversion code
def fetch_player_career_stats(player_id, timeout=None):
    max_retries = 3
    retries = 0
    while retries < max_retries:
        try:
            player_career = playercareerstats.PlayerCareerStats(
                player_id=player_id,
                timeout=timeout
            )
            return player_career.get_data_frames()[0]
        except ReadTimeout as e:
            retries += 1
            print(f"Read timeout. Retrying... (Attempt {retries}/{max_retries})")
    raise Exception("Failed to fetch data after multiple retries.")

# Timeout
timeout = 100

# Get all players
all_players = players.get_players()

# Initialize an empty list to store player dfs
career_dfs = []

# using tqdm to measure conversion process
for player in tqdm(all_players, desc="Processing Players"):
    player_id = player['id']

    # Get player career stats with custom settings
    try:
        career_df = fetch_player_career_stats(
            player_id=player_id,
            timeout=timeout
        )
    except Exception as e:
        print(f"Error fetching data for Player ID {player_id}: {e}")
        continue

    # make sure df is applicable for our criteria of 5 Y period
    if any((career_df['SEASON_ID'] >= '2018') & (career_df['SEASON_ID'] <= '2023')):

        # Filter df for 2018-2023
        mask = (career_df['SEASON_ID'] >= '2018') & (career_df['SEASON_ID'] <= '2023')
        career_df_filtered = career_df.loc[mask].copy()

        # Add player to df
        career_df_filtered['Player_ID'] = player_id
        career_df_filtered['Player_Name'] = player['full_name']

        # Append df to the list
        career_dfs.append(career_df_filtered)

#combining all dfs
all_players_df = pd.concat(career_dfs, ignore_index=True)

#saving to csv
all_players_df.to_csv('all_players_career_stats_2003_2023.csv', index=False)
