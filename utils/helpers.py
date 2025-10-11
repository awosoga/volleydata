import pandas as pd
import janitor

def clean_aupvb_player_info(player_info):
    """
    Clean a player info DataFrame by renaming columns, reordering
    columns, and changing the columns case_type to snake case.
    
    Parameters
    ----------
    player_info : pd.DataFrame
        The DataFrame containing raw player info data

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame with snake_case column names, reordered
        columns, and renamed fields.
    """
    player_info = player_info.clean_names(case_type='snake')

    column_mapping = {
        'points_as_of_game' : 'cumulative_points',
        'au_total_points' : 'points_this_match',
        'matches_played' : 'played_this_match',
        'home_team_flg' : 'is_home_team',
    }
    player_info = player_info.rename(columns=column_mapping)

    column_order = [
        'season', 'week_number', 'game_number', 'game_date', 'rank',
        'rank_change', 'cumulative_points', 'points_this_match',
        'played_this_match', 'first_name', 'last_name', 'uniform_number', 
        'sets_played', 'kills', 'kills_per_set', 'attack_attempts',
        'attack_errors', 'attack_percentage', 'assists', 'assists_per_set',
        'setting_errors', 'service_aces', 'service_aces_per_set',
        'service_errors', 'total_reception_attempts', 'reception_errors',
        'positive_reception_pct', 'digs', 'digs_per_set', 'blocks',
        'blocks_per_set', 'block_assists', 'block_assists_per_set',
        'primary_position_position_lk', 'primary_position_description',
        'primary_position_short_description', 'secondary_position_position_lk',
        'secondary_position_description', 'secondary_position_short_description',
        'current_roster_status_lk', 'current_roster_status_description',
        'is_home_team', 'team_color', 'home_team_name', 'away_team_name',
        'season_id', 'season_type', 'player_id', 'player_slug',
        'uniform_number_display', 'team_id', 'type', 'stat_type',    
    ]

    # add missing columns with NaN values
    for col in column_order:
        if col not in player_info.columns:
            player_info[col] = pd.NA

    player_info = player_info[column_order]

    player_info['played_this_match'] = player_info['played_this_match'].fillna(0).astype(bool)
    player_info['block_assists'] = player_info['block_assists'].fillna(0)
    player_info['block_assists_per_set'] = player_info['block_assists_per_set'].fillna(0)

    return player_info


def clean_aupvb_leaderboards(leaderboards):
    """
    Clean a leaderboards DataFrame by dropping columns, renaming columns,
    reordering columns, and changing the columns case_type to snake case.
    
    Parameters
    ----------
    player_info : pd.DataFrame
        The DataFrame containing raw leaderboards data

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame with snake_case column names, reordered
        columns, and renamed fields.
    """
    leaderboards = leaderboards.drop(['pointTotals.statPoints', 'pointTotals.mvpPoints', 'pointTotals.winPoints',
                                      'leaderboardHistory', 'leaderboardHistory.weeks', 'leaderboardHistory.games'], axis=1,  errors='ignore')
    leaderboards.columns = [col.split('.')[-1] for col in leaderboards.columns]
    leaderboards = leaderboards.clean_names(case_type="snake")

    column_mapping = {
        'season' : 'season_id',
        'year' : 'season',
        'name' : 'team_name',
        'first_place_wins' : 'first_place_mvp',
        'second_place_wins' : 'second_place_mvp',
        'third_place_wins' : 'third_place_mvp',
        'defensive_mvp_wins' : 'defensive_mvp',
        'interval_wins' : 'set_wins',
        'interval_win_total_points' : 'set_wins_total_points',
        'game_wins' : 'match_win',
        'game_win_total_points' : 'match_win_total_points',
        'color' : 'team_color',
        'seed' : 'team_seed',
        'captain_flg' : 'is_captain',
        'position_lk' : 'primary_position_position_lk',
        'description' : 'primary_position_description',
        'short_description' : 'primary_position_short_description',
        'rank' : 'game_rank',
        'games_played' : 'has_game_experience',
    }
    leaderboards = leaderboards.rename(columns=column_mapping, errors='ignore')

    column_order = [
        'season', 'week_number', 'game_number', 'game_rank', 'first_name', 'last_name', 'uniform_number',
        'total_points', 'mvp_points', 'win_points', 'stat_points', 'first_place_mvp', 'first_place_total_points',
        'second_place_mvp', 'second_place_total_points', 'third_place_mvp', 'third_place_total_points', 'defensive_mvp',
        'defensive_mvp_total_points', 'set_wins', 'set_wins_total_points', 'match_win', 'match_win_total_points',
        'service_aces', 'service_ace_points', 'service_errors', 'service_error_points', 'attack_kills', 
        'attack_kill_points', 'attack_errors', 'attack_error_points','set_assists', 'set_assist_points', 
        'set_errors', 'set_error_points', 'digs', 'dig_points', 'good_receptions', 'good_reception_points',
        'reception_errors', 'reception_error_points', 'block_assists', 'block_assist_points', 'block_stuffs',
        'block_stuff_points', 'points_behind', 'has_extra_inning_stats', 'is_captain', 'roster_status', 
        'primary_position_position_lk', 'primary_position_description', 'primary_position_short_description', 
        'team_name', 'team_color', 'team_seed', 'season_id', 'competitor_id', 'player_id', 'player_slug',
        'uniform_number_display', 'overall_rank', 'overall_rank_change', 'total_au_points', 'percent_change',
        'position_change', 'updated_flg', 'tie_flg', 'missed_games_flg', 'previous_seqno', 'has_game_experience'
    ]

        # add missing columns with NaN values
    for col in column_order:
        if col not in leaderboards.columns:
            leaderboards[col] = pd.NA

    leaderboards = leaderboards[column_order]

    fillna_columns = [
        'first_place_mvp', 'first_place_total_points', 'second_place_mvp',
        'second_place_total_points', 'third_place_mvp', 'third_place_total_points',
        'defensive_mvp', 'defensive_mvp_total_points',
        'service_aces', 'service_ace_points', 'service_errors', 'service_error_points',
        'attack_kills', 'attack_kill_points', 'attack_errors', 'attack_error_points',
        'set_assists', 'set_assist_points', 'set_errors', 'set_error_points',
        'digs', 'dig_points', 'good_receptions', 'good_reception_points',
        'reception_errors', 'reception_error_points',
        'block_assists', 'block_assist_points', 'block_stuffs', 'block_stuff_points'
    ]
    leaderboards[fillna_columns] = leaderboards[fillna_columns].fillna(0)

    mvp_cols = ['first_place_mvp', 'second_place_mvp', 'third_place_mvp', 'defensive_mvp']
    leaderboards[mvp_cols] = leaderboards[mvp_cols].astype(bool)

    return leaderboards


def clean_aupvb_pbp(pbp):
    """
    Clean a play-by-play DataFrame by reordering columns, and changing
    the columns case_type to snake case.
    
    Parameters
    ----------
    pbp : pd.DataFrame
        The DataFrame containing raw pbp data

    Returns
    -------
    pd.DataFrame
        A cleaned DataFrame with snake_case column names, and reordered
        columns.
    """
    pbp = pbp.clean_names(case_type='snake')

    column_mapping = {
        'play_seqno': 'play_sequence_number',
        'dig_dig' : 'dig_success',
    }
    pbp = pbp.rename(columns=column_mapping)

    column_order = [
        'season', 'week_number', 'game_number', 'play_sequence_number', 
        'set_number', 'rally_number', 'play_code', 'narrative_formatted', 
        'player_id', 'serve_ace', 'serve_error', 'serve_continue', 
        'attack_kill', 'attack_error', 'attack_continue', 'pass_good', 
        'pass_error', 'pass_continue', 'dig_success', 'dig_continue', 
        'block_stuff', 'block_assist', 'block_continue', 'set_assist', 
        'set_error', 'set_continue', 'home_team_score', 'away_team_score', 
        'scoring_team_id', 'home_team_id', 'away_team_id', 'set_status_lk', 
        'season_id', 'game_id', 'play_text', 'start_time', 'end_time', 
        'video_minutes', 'video_seconds', 'alternate_video_seconds', 
        'date_time_file_received', 
    ]
    pbp = pbp[column_order]
    
    return pbp

def get_season_id(season):
    """
    Get a season_id from a given season.

    Parameters
    ----------
    season : int
        The season year (e.g., 2023)

    Returns
    -------
    int or None
        The corresponding season ID, or None if season not recognized.
    """
    season_ids = {
        2021 : 3,
        2022 : 11, 
        2023 : 138,
        2024 : 205,
    }
    return season_ids.get(season)