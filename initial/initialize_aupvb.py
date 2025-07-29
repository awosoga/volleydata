import pandas as pd
import requests
import janitor

import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
from extract_aupvb_data import *
from helpers import *

def initialize_aupvb_player_info():
    """
    Initializes and stores player info data for all Athletes Unlimited Pro Volleyball games.

    Returns
    -------
    None
    """
    player_info = pd.DataFrame()
    season = 2021

    for season_id in [3, 11, 138, 205]:
        for game_number in range(1, 31):
            url = f"https://auprosports.com/proxy.php?request=/api/stats/v2/volleyball/{season_id}/by-game/{game_number}?statTypes=volleyball"
            headers = {
                "User-Agent": "Mozilla/5.0"
            }
            response = requests.get(url, headers=headers)
            json = response.json()
            
            new_player_info = extract_aupvb_player_info(json)
            new_player_info['season'] = season
            player_info = pd.concat([player_info, new_player_info], ignore_index=True)
        season += 1

    player_info = clean_aupvb_player_info(player_info)
    player_info.to_csv('aupvb_player_info.csv', index=False)


def initialize_aupvb_leaderboards():
    """
    Initializes and stores leaderboards data for all Athletes Unlimited Pro Volleyball games.
    
    Returns
    -------
    None
    """
    leaderboards = pd.DataFrame()

    for season_id in [3, 11, 138, 205]:
        for game_number in range(1, 31):
            url = f"https://auprosports.com/proxy.php?request=/api/leaderboard/v2/volleyball/type/game?gameNumber={game_number}%26season={season_id}"
            headers = {
                "User-Agent": "Mozilla/5.0"
            }
            response = requests.get(url, headers=headers)
            json = response.json()
            
            leaderboards = pd.concat([leaderboards, extract_aupvb_leaderboards(json)], ignore_index=True)

    leaderboards = clean_aupvb_leaderboards(leaderboards)
    leaderboards.to_csv('aupvb_leaderboards.csv', index=False)


def initialize_aupvb_pbp():
    """
    Initializes and stores pbp data for all Athletes Unlimited Pro Volleyball games.
    
    Returns
    -------
    None
    """
    season = 2021
    for season_id in [3, 11, 138, 205]:
        pbp = pd.DataFrame()
        for game_number in range(1, 31):
            url = f"https://auprosports.com/proxy.php?request=/api/play-by-play/v2/volleyball/event/{season_id}/{game_number}"
            headers = {
                "User-Agent": "Mozilla/5.0"
            }
            response = requests.get(url, headers=headers)
            json = response.json()
            
            new_pbp = extract_aupvb_pbp(json)
            new_pbp['season'] = season
            new_pbp['season_id'] = season_id
            pbp = pd.concat([pbp, new_pbp], ignore_index=True)

        pbp = clean_aupvb_pbp(pbp)
        pbp.to_csv(f'aupvb_pbp_{season}.csv', index=False)
        season += 1