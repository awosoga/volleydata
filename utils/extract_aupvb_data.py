import pandas as pd
from datetime import datetime


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