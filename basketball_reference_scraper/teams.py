import pandas as pd
from bs4 import BeautifulSoup
import requests

try:
    from constants import TEAM_TO_TEAM_ABBR, TEAM_SET, get_team_set
    from utils import remove_accents, RetriableRequest
except:
    from basketball_reference_scraper.constants import TEAM_TO_TEAM_ABBR, TEAM_SETS,get_team_set
    from basketball_reference_scraper.utils import remove_accents, RetriableRequest

    from basketball_reference_scraper.constants import CATEGORY_COLUMNS, TEAM_CATEGORIES

def get_roster(team, season_end_year):
    r = RetriableRequest.get(
        f'https://www.basketball-reference.com/teams/{team}/{season_end_year}.html')
    if r.status_code!=200:
        teamSet = get_team_set(team)
        if teamSet:
            for teamCode in teamSet:
                r =  RetriableRequest.get( f'https://www.basketball-reference.com/teams/{teamCode}/{season_end_year}.html')
                if r.status_code ==200:
                    break
        
    df = None
    
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table' ,attrs={'id': 'roster'})
        df = pd.read_html(str(table))[0]
        df.columns = ['NUMBER', 'PLAYER', 'POS', 'HEIGHT', 'WEIGHT', 'BIRTH_DATE',
                      'NATIONALITY', 'EXPERIENCE', 'COLLEGE']
        # remove rows with no player name (this was the issue above)
        df = df[df['PLAYER'].notna()]
        df['PLAYER'] = df['PLAYER'].apply(
            lambda name: remove_accents(name, team, season_end_year))
        
        # handle rows with empty fields but with a player name.
        df['BIRTH_DATE'] = df['BIRTH_DATE'].apply(
            lambda x: pd.to_datetime(x) if pd.notna(x) else pd.NaT)
        df['NATIONALITY'] = df['NATIONALITY'].apply(
            lambda x: x.upper() if pd.notna(x) else '')

    return df


def get_team_stats( season_end_year, team = None, data_format= None):
    selector = None
    if data_format:
        data_format = data_format.upper()
        if data_format == 'TOTALS':
            selector = 'totals-team'
        elif data_format == 'PER_GAME':
            selector = 'per_game-team'
        elif data_format == 'PER_POSS':
            selector = 'per_poss-team'
        elif data_format == 'SHOOTING':
            selector = 'shooting-team'
        elif data_format == 'ADVANCED':
            selector = 'advanced-team'
    r = RetriableRequest.get(
        f'https://www.basketball-reference.com/leagues/NBA_{season_end_year}.html')
    df = None
    
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        def retrieve(selector):
            table = soup.find('table',attrs={'id':selector})
            df = pd.read_html(str(table))[0]
            if selector == "advanced-team":
                df = df.droplevel(0, axis=1)
            elif selector == "shooting-team":
                newCols = []
                for col in df.columns:
                    if col[0] == "% of FGA by Distance":
                        newCols.append("% of FGA " +col[1])
                    elif col[0] == "FG% by Distance":
                        newCols.append(col[1] + " FG%")
                    elif col[0] == "% of FG Ast'd":
                        newCols.append(col[1] + " % AST" )
                    elif col[0] == "Dunks":
                        newCols.append(col[1] + " Dunks")
                    elif col[0] == "Corner":
                        newCols.append(col[1] + " Corner 3s")
                    elif col[0] == "Layups":
                        newCols.append(col[1] + " Layups")
                    elif col[0] == "Heaves":
                        newCols.append(col[1] + " Heaves")
                    else:
                        newCols.append(col[1])
                df.columns = newCols
            if selector != 'per_poss-team':
                league_avg_index = df[df['Team'] == 'League Average'].index[0]
                df = df[:league_avg_index]
            df['Team'] = df['Team'].apply(lambda x: x.replace('*', '').upper())
            df['TEAM'] = df['Team'].apply(lambda x: TEAM_TO_TEAM_ABBR[x])
            
            df = df.drop(['Rk', 'Team'], axis=1)
            df.loc[:, 'SEASON'] = f'{season_end_year-1}-{str(season_end_year)[2:]}'
            cols = df.columns.tolist()
            cols = cols[-2:] + cols[:-2]  # Move the last two columns to the front
            df = df[cols]
            df = df.dropna(how='all')
            df = df.dropna(axis=1,how='all')
            if team:
                s = df[df['TEAM'] == team]
                return pd.Series(index=list(s.columns), data=s.values.tolist()[0])
            return df
        
        if selector:
            return retrieve(selector)
        else:
            stat_dfs = {}
            for category in TEAM_CATEGORIES:
                stat_dfs[category] = retrieve(category+"-team")
            return stat_dfs


def get_opp_stats(team, season_end_year, data_format='PER_GAME'):
    if data_format == 'TOTAL':
        selector = 'totals-opponent'
    elif data_format == 'PER_GAME':
        selector = 'per_game-opponent'
    elif data_format == 'PER_POSS':
        selector = 'per_poss-opponent'
    else:
        selector = 'shooting-opponent'
    r = RetriableRequest.get(
        f'https://www.basketball-reference.com/leagues/NBA_{season_end_year}.html')
    df = None
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table', attrs={'id':selector})
        df = pd.read_html(str(table))[0]
        if selector == "shooting-opponent":
            newCols = []
            for col in df.columns:
                if col[0] == "% of FGA by Distance":
                    newCols.append("% of FGA " +col[1])
                elif col[0] == "FG% by Distance":
                    newCols.append(col[1] + " FG%")
                elif col[0] == "% of FG Ast'd":
                    newCols.append(col[1] + " % AST" )
                elif col[0] == "Dunks":
                    newCols.append(col[1] + " Dunks")
                elif col[0] == "Corner":
                    newCols.append(col[1] + " Corner 3s")
                elif col[0] == "Layups":
                    newCols.append(col[1] + " Layups")
                elif col[0] == "Heaves":
                    newCols.append(col[1] + " Heaves")
                else:
                    newCols.append(col[1])
            df.columns = newCols
        league_avg_index = df[df['Team'] == 'League Average'].index[0]
        df = df[:league_avg_index]
        df['Team'] = df['Team'].apply(lambda x: x.replace('*', '').upper())
        df['TEAM'] = df['Team'].apply(lambda x: TEAM_TO_TEAM_ABBR[x])
        df = df.drop(['Rk', 'Team'], axis=1)
        df.columns = list(map(lambda x: 'OPP_'+x, list(df.columns)))
        df.rename(columns={'OPP_TEAM': 'TEAM'}, inplace=True)
        df.loc[:, 'SEASON'] = f'{season_end_year-1}-{str(season_end_year)[2:]}'
        s = df[df['TEAM'] == team]
        return pd.Series(index=list(s.columns), data=s.values.tolist()[0])


def get_team_misc(team, season_end_year):
    r = RetriableRequest.get(
        f'https://www.basketball-reference.com/teams/{team}/{season_end_year}.html')
    df = None
    html_content = r.text
    html_content = html_content.replace("<!--", "")
    html_content = html_content.replace("-->", "")
    if r.status_code == 200:
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table',attrs={'id':'team_misc'})
        df = pd.read_html(str(table))[0]
        
        df = df.droplevel(0, axis=1)
        df.set_index(df.columns[0], inplace=True)
        df.index.name = None
        return df


def get_roster_stats(team, season_end_year: int, data_format='PER_GAME', playoffs=False):
    if playoffs:
        period = 'playoffs'
    else:
        period = 'leagues'
    selector = data_format.lower()
    
    correctTeamSet = None
    for s in TEAM_SETS:
        if team in s:
            correctTeamSet = s
            break
    for teamCode in s: 
        r = RetriableRequest.get(
        f'https://www.basketball-reference.com/teams/{teamCode}/{season_end_year}.html')
        if r.status_code == 200:
            break
    
    html_content = r.text
    html_content = html_content.replace("<!--", "")
    html_content = html_content.replace("-->", "")
    df = None
    if r.status_code == 200:
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table',attrs={'id':selector})
        df = pd.read_html(str(table))[0]
        df.rename(columns={'Player': 'PLAYER', 'Age': 'AGE', 'Pos': 'POS'}, inplace=True)
        if selector == "adj_shooting":
                df = df.droplevel(0, axis=1)
        if selector == "shooting":
            newCols = []
            for col in df.columns:
                if col[0] == "% of FGA by Distance":
                    newCols.append("% of FGA " +col[1])
                elif col[0] == "FG% by Distance":
                    newCols.append(col[1] + " FG%")
                elif col[0] == "% of FG Ast'd":
                    newCols.append(col[1] + " % AST" )
                elif col[0] == "Dunks":
                    newCols.append(col[1] + " Dunks")
                elif col[0] == "Corner 3s":
                    newCols.append(col[1] + " Corner 3s")
                elif col[0] == "Layups":
                    newCols.append(col[1] + " Layups")
                elif col[0] == "Heaves":
                    newCols.append(col[1] + " Heaves")
                else:
                    newCols.append(col[1])
            df.columns = newCols
        df = df[df['PLAYER'] != 'Team']
        df = df[df['PLAYER'] != 'League Average']
        df.loc[:, 'SEASON'] = f'{season_end_year-1}-{str(season_end_year)[2:]}'
        df = df.dropna(how='all')
        df = df.dropna(axis=1,how='all')
        
        df['PLAYER'] = df['PLAYER'].apply(
            lambda name: remove_accents(name, team, season_end_year))
        df = df.reset_index().drop(['Rk', 'index'], axis=1)
        return df

def get_team_ratings(*, team=[], season_end_year: int):

    # Scrape data from URL
    r = RetriableRequest.get(
        f'https://www.basketball-reference.com/teams/{team}/{season_end_year}.html')
    if r.status_code == 200:
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table')
        df = pd.read_html(str(table))[0]

        # Clean columns and indexes
        df = df.droplevel(level=0, axis=1)

        df.drop(columns=['Rk', 'Conf', 'Div', 'W', 'L', 'W/L%'], inplace=True)
        upper_cols = list(pd.Series(df.columns).apply(lambda x: x.upper()))
        df.columns = upper_cols

        df['TEAM'] = df['TEAM'].apply(lambda x: x.upper())
        df['TEAM'] = df['TEAM'].apply(lambda x: TEAM_TO_TEAM_ABBR[x])

        # Add 'Season' column in and change order of columns
        df['SEASON'] = f'{season_end_year-1}-{str(season_end_year)[2:]}'
        cols = df.columns.tolist()
        cols = cols[0:1] + cols[-1:] + cols[1:-1]
        df = df[cols]

        # Add the ability to either pass no teams (empty list), one team (str), or multiple teams (list)
        if len(team) > 0:
            if isinstance(team, str):
                list_team = []
                list_team.append(team)
                df = df[df['TEAM'].isin(list_team)]
            else:
                df = df[df['TEAM'].isin(team)]

    return df
