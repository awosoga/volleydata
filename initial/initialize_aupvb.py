import pandas as pd
import requests
import janitor
from datetime import datetime

import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
from extract_aupvb_data import *
from helpers import *
from upload_to_releases import upload_to_releases


def initialize_aupvb_player_info():
    """
    Initializes and stores player info data for all Athletes Unlimited Pro Volleyball games.

    Returns
    -------
    None
    """
    player_info = pd.DataFrame()

    year = datetime.now().year

    for season in list(range(2021, year)):
        season_id = get_season_id(season)
        for game_number in range(1, 31):
            url = f'https://auprosports.com/proxy.php?request=/api/stats/v2/volleyball/{season_id}/by-game/{game_number}?statTypes=volleyball'
            headers = {
                'User-Agent': 'Mozilla/5.0'
            }
            response = requests.get(url, headers=headers)
            json = response.json()
            
            new_player_info = extract_aupvb_player_info(json)
            new_player_info['season'] = season
            player_info = pd.concat([player_info, new_player_info], ignore_index=True)

    player_info = clean_aupvb_player_info(player_info)
    player_info.to_csv('aupvb_player_info.csv', index=False)
    upload_to_releases('aupvb_player_info.csv', 'aupvb-player-info')


def initialize_aupvb_leaderboards():
    """
    Initializes and stores leaderboards data for all Athletes Unlimited Pro Volleyball games.
    
    Returns
    -------
    None
    """
    leaderboards = pd.DataFrame()

    year = datetime.now().year

    for season in list(range(2021, year)):
        season_id = get_season_id(season)
        for game_number in range(1, 31):
            url = f'https://auprosports.com/proxy.php?request=/api/leaderboard/v2/volleyball/type/game?gameNumber={game_number}%26season={season_id}'
            headers = {
                'User-Agent': 'Mozilla/5.0'
            }
            response = requests.get(url, headers=headers)
            json = response.json()
            
            leaderboards = pd.concat([leaderboards, extract_aupvb_leaderboards(json)], ignore_index=True)

    leaderboards = clean_aupvb_leaderboards(leaderboards)
    leaderboards.to_csv('aupvb_leaderboards.csv', index=False)
    upload_to_releases('aupvb_leaderboards.csv', 'aupvb-leaderboards')


def initialize_aupvb_pbp():
    """
    Initializes and stores pbp data for all Athletes Unlimited Pro Volleyball games.
    
    Returns
    -------
    None
    """

    year = datetime.now().year

    for season in list(range(2021, year)):
        season_id = get_season_id(season)

        pbp = pd.DataFrame()
        for game_number in range(1, 31):
            url = f'https://auprosports.com/proxy.php?request=/api/play-by-play/v2/volleyball/event/{season_id}/{game_number}'
            headers = {
                'User-Agent': 'Mozilla/5.0'
            }
            response = requests.get(url, headers=headers)
            json = response.json()
            
            new_pbp = extract_aupvb_pbp(json)
            new_pbp['season'] = season
            new_pbp['season_id'] = season_id
            pbp = pd.concat([pbp, new_pbp], ignore_index=True)

        pbp = clean_aupvb_pbp(pbp)
        file_name = 'aupvb_pbp_' + str(season) + '.csv'
        pbp.to_csv(file_name, index=False)
        upload_to_releases(file_name, 'aupvb-pbp')