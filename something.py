from basketball_reference_scraper.players import get_stats, get_game_logs, get_player_headshot
from basketball_reference_scraper.teams import get_roster, get_team_stats, get_opp_stats, get_roster_stats, get_team_misc
from basketball_reference_scraper.pbp import get_pbp
from basketball_reference_scraper.shot_charts import get_shot_chart
from basketball_reference_scraper.box_scores import get_box_scores
from basketball_reference_scraper.seasons import get_schedule, get_standings
from basketball_reference_scraper.utils import get_player_suffix, RetriableRequest
from basketball_reference_scraper.lookup import lookup
from basketball_reference_scraper.constants import CATEGORY_COLUMNS, CATEGORIES, TEAM_CATEGORIES, TEAM_SETS
import pandas as pd
import time
import os

import unicodedata, unidecode
YEAR_TEAM_ROSTER = {}


# read player_data from concerted files
player_dfs = {
    "per_game": pd.DataFrame(),
    "totals": pd.DataFrame(),
    "per_minute": pd.DataFrame(),
    "per_poss": pd.DataFrame(),
    "advanced": pd.DataFrame(),
    "adj_shooting": pd.DataFrame(),
    "shooting": pd.DataFrame(),
    "game_logs": pd.DataFrame()
}

base_dir = 'player_data'
# Read the CSV file into a DataFrame
for category in player_dfs:
    file_path = os.path.join(base_dir,f'{category}.csv')
    player_dfs[category] = pd.read_csv(file_path)


# read game_logs
game_logs_dfs = {
    "game_logs": pd.DataFrame(),
    "advanced": pd.DataFrame(),
    "playoffs":pd.DataFrame()
}
base_dir = 'game_logs'
for filename in os.listdir(base_dir):
    file_path = os.path.join(base_dir,filename)
    if 'advanced' in filename:
        game_logs_dfs['advanced'] = pd.concat([game_logs_dfs['advanced'],pd.read_csv(file_path)])
    elif 'playoff' in filename:
        game_logs_dfs['playoffs'] = pd.concat([game_logs_dfs['playoffs'],pd.read_csv(file_path)])
    else:
        game_logs_dfs['game_logs'] = pd.concat([game_logs_dfs['game_logs'],pd.read_csv(file_path)])


# convert age: year - days -> year
game_logs_dfs['game_logs']['AGE'] = game_logs_dfs['game_logs']['AGE'].apply(lambda x: int(x[:2]) + float(int(x.split('-')[1])/365))
# refactor RESULT into WON and diff
import re
game_logs_dfs['game_logs']['DIFF'] = game_logs_dfs['game_logs']['RESULT'].apply(lambda text: float(re.search(r'\((.*?)\)', text).group(1)))
game_logs_dfs['game_logs']['WON'] = game_logs_dfs['game_logs']['RESULT'].apply(lambda x: 1 if x[0] =='W' else 0 )
# refactor home into hot encode
game_logs_dfs['game_logs']['HOME']= game_logs_dfs['game_logs']['HOME/AWAY'].apply(lambda x: 1 if x=='HOME' else 0 )

# drop unneeded columns
game_logs_dfs['game_logs'].drop(['RESULT','HOME/AWAY'],axis = 1, inplace = True)

# get rid of NANs
game_logs_dfs['game_logs'].fillna(0,inplace=True)

# convert columns to float values
float_columns = ['GS','FG','FGA','FG%','3P','3PA','3P%','FT','FTA','FT%','ORB','DRB','TRB','AST','STL','BLK','TOV','PF','PTS','GAME_SCORE','+/-']
for column in float_columns:
    game_logs_dfs['game_logs'][column] = pd.to_numeric(game_logs_dfs['game_logs'][column], errors='coerce')
    
game_logs_dfs['game_logs']['DATE'] = pd.to_datetime(game_logs_dfs['game_logs']['DATE'])
game_logs_dfs['game_logs']



# read player_ids
from unidecode import unidecode
import json
with open('player_ids.csv', 'r') as file:
    player_ids = json.load(file)
game_logs_dfs['game_logs']['PLAYER_ID'] = game_logs_dfs['game_logs']['PLAYER'].apply(lambda x: player_ids[unidecode(x)]) 

# read team_ids
import json
with open('team_ids.csv', 'r') as file:
    team_ids = json.load(file)


