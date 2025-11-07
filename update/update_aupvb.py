import pandas as pd
import requests
import re
from datetime import datetime

import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
from extract_aupvb_data import *
from helpers import *
from upload_to_releases import upload_to_releases

def update_aupvb_player_info():
    """
    Updates and stores player info data for all Athletes Unlimited Pro Volleyball games.

    Returns
    -------
    None
    """

    current_year = datetime.now().year
    current_schedule = extract_aupvb_schedule()
    current_schedule = current_schedule.query("season == @current_year")
    
    # Determine current game number and filter schedule
    current_game = current_schedule['currentGame'].max()
    # if current_game is null, set it to max game_number
    current_game = current_game if pd.notnull(current_game) else current_schedule['game_number'].max()

    current_schedule = current_schedule[current_schedule['game_number'] < current_game]
    
    try:
        player_info = pd.read_csv('https://github.com/awosoga/volleydata/releases/download/aupvb-player-info/aupvb_player_info.csv')
    except Exception as e:
        player_info = pd.DataFrame()

    updated_player_info = pd.DataFrame()
    season = current_schedule['season'].iloc[0]
    season_id = current_schedule['seasonId'].iloc[0]
    games_to_fetch = current_schedule['game_number'].tolist()

    for game_number in games_to_fetch:
        #print(f'Fetching data for season {season}, game {game_number}')

        # check if game has already been fetched for the current season id
        if not player_info.empty and player_info.query("season == @season and game_number == @game_number").empty is False: 
            continue

        url = f'https://auprosports.com/proxy.php?request=/api/stats/v2/volleyball/{season_id}/by-game/{game_number}?statTypes=volleyball'
        headers = {
            'User-Agent': 'Mozilla/5.0'
        }
        response = requests.get(url, headers=headers)
        json = response.json()

        new_player_info = extract_aupvb_player_info(json)
        new_player_info['season'] = season

        updated_player_info = pd.concat([updated_player_info, new_player_info], ignore_index=True)

    if not updated_player_info.empty:
        updated_player_info = clean_aupvb_player_info(updated_player_info)
        player_info = pd.concat([player_info, updated_player_info], ignore_index=True)
        player_info.to_csv('aupvb_player_info.csv', index=False)
        upload_to_releases('aupvb_player_info.csv', 'aupvb-player-info')

def update_aupvb_leaderboards():
    """
    Updates and stores leaderboards data for all Athletes Unlimited Pro Volleyball games.

    Returns
    -------
    None
    """
    current_year = datetime.now().year
    current_schedule = extract_aupvb_schedule()
    current_schedule = current_schedule.query("season == @current_year")
    
    # Determine current game number and filter schedule
    current_game = current_schedule['currentGame'].max()
        # if current_game is null, set it to max game_number
    current_game = current_game if pd.notnull(current_game) else current_schedule['game_number'].max()

    current_schedule = current_schedule[current_schedule['game_number'] < current_game]

    try:
        leaderboards = pd.read_csv('https://github.com/awosoga/volleydata/releases/download/aupvb-leaderboards/aupvb_leaderboards.csv')
    except Exception as e:
        leaderboards = pd.DataFrame()

    updated_leaderboards = pd.DataFrame()
    season = current_schedule['season'].iloc[0]
    season_id = current_schedule['seasonId'].iloc[0]
    games_to_fetch = current_schedule['game_number'].tolist()

    for game_number in games_to_fetch:
        #print(f'Fetching data for season {season}, game {game_number}')

        # check if game has already been fetched for the current season id
        if not leaderboards.empty and leaderboards.query("season == @season and game_number == @game_number").empty is False: 
            continue

        url = f'https://auprosports.com/proxy.php?request=/api/leaderboard/v2/volleyball/type/game?gameNumber={game_number}%26season={season_id}'
        headers = {
            'User-Agent': 'Mozilla/5.0'
        }
        response = requests.get(url, headers=headers)
        json = response.json()

        new_leaderboards = extract_aupvb_leaderboards(json)
        new_leaderboards['season'] = season

        updated_leaderboards = pd.concat([updated_leaderboards, new_leaderboards], ignore_index=True)

    if not updated_leaderboards.empty:
        updated_leaderboards = clean_aupvb_leaderboards(updated_leaderboards)
        leaderboards = pd.concat([leaderboards, updated_leaderboards], ignore_index=True)
        leaderboards.to_csv('aupvb_leaderboards.csv', index=False)
        upload_to_releases('aupvb_leaderboards.csv', 'aupvb-leaderboards')

def update_aupvb_pbp():
    """
    Updates and stores play by play data for all Athletes Unlimited Pro Volleyball games.

    Returns
    -------
    None
    """
    current_year = datetime.now().year
    current_schedule = extract_aupvb_schedule()
    current_schedule = current_schedule.query("season == @current_year")
    
    # Determine current game number and filter schedule
    current_game = current_schedule['currentGame'].max()
    # if current_game is null, set it to max game_number
    current_game = current_game if pd.notnull(current_game) else current_schedule['game_number'].max()

    current_schedule = current_schedule[current_schedule['game_number'] < current_game]

    try:
        pbp = pd.read_csv(f'https://github.com/awosoga/volleydata/releases/download/aupvb-pbp/aupvb_pbp_{current_year}.csv')
    except Exception as e:
        pbp = pd.DataFrame()

    updated_pbp = pd.DataFrame()
    season = current_schedule['season'].iloc[0]
    season_id = current_schedule['seasonId'].iloc[0]
    games_to_fetch = current_schedule['game_number'].tolist()

    for game_number in games_to_fetch:
        #print(f'Fetching data for season {season}, game {game_number}')

        # check if game has already been fetched for the current season id
        if not pbp.empty and pbp.query("season == @season and game_number == @game_number").empty is False: 
            continue

        url = f'https://auprosports.com/proxy.php?request=/api/play-by-play/v2/volleyball/event/{season_id}/{game_number}'
        headers = {
            'User-Agent': 'Mozilla/5.0'
        }
        response = requests.get(url, headers=headers)
        json = response.json()

        new_pbp = extract_aupvb_pbp(json)
        new_pbp['season'] = season
        new_pbp['season_id'] = season_id

        updated_pbp = pd.concat([updated_pbp, new_pbp], ignore_index=True)

    if not updated_pbp.empty:
        updated_pbp = clean_aupvb_pbp(updated_pbp)
        pbp = pd.concat([pbp, updated_pbp], ignore_index=True)
        pbp = clean_aupvb_pbp(pbp)
        file_name = 'aupvb_pbp_' + str(season) + '.csv'
        pbp.to_csv(file_name, index=False)
        upload_to_releases(file_name, 'aupvb-pbp')


# Run the updates
if __name__ == "__main__":
    update_aupvb_player_info()
    update_aupvb_leaderboards()
    update_aupvb_pbp()