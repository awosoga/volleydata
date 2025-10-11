import pandas as pd
from datetime import datetime
import json
import requests

def extract_aupvb_player_info(json):
    """
    Extract player information from Athletes Unlimited Pro Volleyball game JSON data.

    Parameters
    ----------
    json : dict
        JSON object containing Athletes Unlimited Pro Volleyball game data.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing player information for the game.
    """
    player_info = pd.json_normalize(json.get('data', []))
    player_stats = pd.DataFrame()
    
    for _, row in player_info.iterrows():
        if isinstance(row['stats'], list) and len(row['stats']) > 0:
            stats_df = pd.json_normalize(row['stats'][0])
            player_stats = pd.concat([player_stats, stats_df], ignore_index=True)

    player_info = pd.concat([player_info.drop('stats', axis=1), player_stats], axis=1)
    player_info = player_info.loc[:, ~player_info.columns.duplicated()]

    return player_info


def extract_aupvb_leaderboards(json):
    """
    Extract leaderboard information from Athletes Unlimited Pro Volleyball game Json data.

    Parameters
    ----------
    json : dict
        JSON object containing Athletes Unlimited Pro Volleyball game data.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing leaderboard information for the game.
    """
    leaderboards = pd.json_normalize(json.get('leaderboard', {}).get('leaderboards', []))

    for game_metadata in ['season', 'gameNumber', 'weekNumber', 'year']:
        leaderboards[game_metadata] = json.get('leaderboard', {}).get(game_metadata)

    return leaderboards


def extract_aupvb_pbp(json):

    """
    Extract play by play information from Athletes Unlimited Pro Volleyball game Json data.

    Parameters
    ----------
    json : dict
        JSON object containing Athletes Unlimited Pro Volleyball game data.
    """
    pbp = pd.json_normalize(json.get('data', [])[0].get('plays', []))
    pbp['week_number'] = -(-pbp['gameNumber'] // 6)

    return pbp

def extract_aupvb_schedule():
    """
    Extract Athletes Unlimited Pro Volleyball schedule data for a given year.

    Parameters
    ----------
    year : int
        The year for which to extract the schedule data.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the schedule information for the specified year.
    """
    url = "https://auprosports.com/proxy.php?request=api/seasons/volleyball/v1"
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    schedule_json = response.json()

    schedule = pd.json_normalize(schedule_json.get('data', []))
    schedule['season'] = schedule['description'].str.extract(r'(\d{4})').astype(int)
    schedule = schedule.explode('gameIds')
    
    # Group by season and create ascending game number within each group
    schedule['game_number'] = schedule.groupby('season').cumcount() + 1

    return schedule
