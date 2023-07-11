import pandas as pd
from bs4 import BeautifulSoup

try:
    from utils import get_player_suffix, RetriableRequest
    from lookup import lookup
except:
    from basketball_reference_scraper.utils import get_player_suffix, RetriableRequest
    from basketball_reference_scraper.lookup import lookup
    
    from basketball_reference_scraper.constants import CATEGORY_COLUMNS, CATEGORIES




def get_stats(_name= None, stat_type=None, playoffs=False, career=False, ask_matches = True, suffix = None):
    
    if not suffix:
        name = lookup(_name, ask_matches)
        suffix = get_player_suffix(name)[:-5]
    selector = None
    if stat_type:
        selector = stat_type.lower()
        if playoffs:
            selector = 'playoffs_'+selector
    r = RetriableRequest.get(f'https://www.basketball-reference.com{suffix}.html')
    if r.status_code==200:
        html_content = r.text
        html_content = html_content.replace("<!--", "")
        html_content = html_content.replace("-->", "")
        soup = BeautifulSoup(html_content, 'html.parser')
        
        def retrieve(selector):
            table = soup.find('table', attrs={'id': selector})
            if table is None:
                return pd.DataFrame()
            df = pd.read_html(str(table))[0]
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
            df.rename(columns={'Season': 'SEASON', 'Age': 'AGE','Tm':'TEAM',
                    'Team': 'TEAM', 'Lg': 'LEAGUE', 'Pos': 'POS'}, inplace=True)
            
            if 'FG.1' in df.columns:
                df.rename(columns={'FG.1': 'FG%'}, inplace=True)
            if 'eFG' in df.columns:
                df.rename(columns={'eFG': 'eFG%'}, inplace=True)
            if 'FT.1' in df.columns:
                df.rename(columns={'FT.1': 'FT%'}, inplace=True)
            career_index = df[df['SEASON']=='Career'].index[0]
            if career:
                df = df.iloc[career_index+2:, :]
            else:
                df = df.iloc[:career_index, :]
            df = df.reset_index().drop('index', axis=1)
            df = df[CATEGORY_COLUMNS[selector]]
            df = df.fillna(0)
            return df
        if selector:
            return retrieve(selector)
        else:
            player_dfs = {}
            for category in CATEGORIES:
                player_dfs[category] = retrieve(category)
            return player_dfs
    else:
        print(r.status_code)
        print(r.content)

def isNumHyph(s):
    list = "1234567890-"
    for char in s:
        if char not in list:
            return False
    return True

def get_game_logs(_name = None,year = None, playoffs=False, ask_matches=True,suffix = None,advanced = False):
    if not suffix:
        name = lookup(_name, ask_matches)
        print(name)
        suffix = get_player_suffix(name)[:-5]
    if advanced:
        selector = 'pgl_advanced'
    else:
        selector = 'pgl_basic'
    if year:
        r = RetriableRequest.get(f'https://www.basketball-reference.com/{suffix}/gamelog{"-advanced" if advanced else ""}/{year}')
        
        if r.status_code==200:
            html_content = r.text
            html_content = html_content.replace("<!--", "")
            html_content = html_content.replace("-->", "")
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find('table', attrs={'id': selector})
            df = None
            if table:
                df = pd.read_html(str(table))[0]
                df.rename(columns = {'Date': 'DATE', 'Age': 'AGE', 'Tm': 'TEAM', 'Unnamed: 5': 'HOME/AWAY', 'Opp': 'OPPONENT',
                    'Unnamed: 7': 'RESULT', 'GmSc': 'GAME_SCORE'}, inplace=True)
                df['HOME/AWAY'] = df['HOME/AWAY'].apply(lambda x: 'AWAY' if x=='@' else 'HOME')
                df = df[df['Rk']!='Rk']
                df = df.drop(['Rk', 'G'], axis=1)
                df['DATE'] = pd.to_datetime(df['DATE'])
                df = df[~(df == 'Did Not Play').any(axis=1)]
                df = df[~(df == 'Inactive').any(axis=1)]
            df_p = None   
            if playoffs == True:
                selector = 'pgl_basic_playoffs'
                table = soup.find('table', attrs={'id': selector})
                if table:
                    df_p = pd.read_html(str(table))[0]
                    
                    df_p.rename(columns={'Date': 'DATE', 'Age': 'AGE', 'Tm': 'TEAM', 'Unnamed: 5': 'HOME/AWAY', 'Opp': 'OPPONENT',
                                        'Unnamed: 7': 'RESULT', 'GmSc': 'GAME_SCORE'}, inplace=True)
                    df_p['HOME/AWAY'] = df_p['HOME/AWAY'].apply(lambda x: 'AWAY' if x == '@' else 'HOME')
                    df_p = df_p[df_p['Rk'] != 'Rk']
                    df_p = df_p.drop(['Rk', 'G'], axis=1)
                    df_p['DATE'] = pd.to_datetime(df_p['DATE'])
                    df_p = df_p[~(df_p == 'Did Not Play').any(axis=1)]
                    df_p = df_p[~(df_p == 'Inactive').any(axis=1)]

            return df if df_p is None else (df,df_p)

                

    else:
        print('getting big batch')
        r = RetriableRequest.get(f'https://www.basketball-reference.com/{suffix}.html')
        html_content = r.text
        html_content = html_content.replace("<!--", "")
        html_content = html_content.replace("-->", "")
        soup = BeautifulSoup(html_content, 'html.parser')
        advanced_game_logs =  pd.DataFrame()
        normal_game_logs = pd.DataFrame()
        playoff_game_logs = pd.DataFrame()
        game_log_years = set([tag.text for tag in soup.find_all('a', href=lambda href: href and suffix in href and 'gamelog' in href) if isNumHyph(tag.text) ])
        
        
        print(game_log_years)
        for season in game_log_years:
            # print(f'getting 20{season[-2:]} logs', end='\r')
            values = get_game_logs(suffix=suffix, playoffs=True, year=f'20{season[-2:]}')
            if type(values) is tuple:
                current_normal, current_playoff = values
            else:
                current_normal, current_playoff = values, pd.DataFrame()
            playoff_game_logs = pd.concat([playoff_game_logs, current_playoff])
            normal_game_logs = pd.concat([normal_game_logs, current_normal])
            
            advanced_current = get_game_logs(suffix=suffix, advanced=True, year=f'20{season[-2:]}')
            advanced_game_logs = pd.concat([advanced_game_logs, advanced_current])
    
        game_logs = {
            "game_logs": normal_game_logs,
            "advanced_game_logs": advanced_game_logs,
            "playoff_game_logs": playoff_game_logs
        } 

        return game_logs


def get_player_headshot(_name, ask_matches=True):
    name = lookup(_name, ask_matches)
    suffix = get_player_suffix(name)
    jpg = suffix.split('/')[-1].replace('html', 'jpg')
    url = 'https://d2cwpp38twqe55.cloudfront.net/req/202006192/images/players/'+jpg
    return url

def get_player_splits(_name, season_end_year, stat_type='PER_GAME', ask_matches=True):
    name = lookup(_name, ask_matches)
    suffix = get_player_suffix(name)[:-5]
    r = RetriableRequest.get(f'https://www.basketball-reference.com/{suffix}/splits/{season_end_year}')
    if r.status_code==200:
        html_content = r.text
        html_content = html_content.replace("<!--", "")
        html_content = html_content.replace("-->", "")
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        if table:
            df = pd.read_html(str(table))[0]
            for i in range(1, len(df['Unnamed: 0_level_0','Split'])):
                if isinstance(df['Unnamed: 0_level_0','Split'][i], float):
                    df['Unnamed: 0_level_0','Split'][i] = df['Unnamed: 0_level_0','Split'][i-1]
            df = df[~df['Unnamed: 1_level_0','Value'].str.contains('Total|Value')]

            headers = df.iloc[:,:2]
            headers = headers.droplevel(0, axis=1)

            if stat_type.lower() in ['per_game', 'shooting', 'advanced', 'totals']:
                if stat_type.lower() == 'per_game':
                    df = df['Per Game']
                    df['Split'] = headers['Split']
                    df['Value'] = headers['Value']
                    cols = df.columns.tolist()
                    cols = cols[-2:] + cols[:-2]
                    df = df[cols]
                    return df
                elif stat_type.lower() == 'shooting':
                    df = df['Shooting']
                    df['Split'] = headers['Split']
                    df['Value'] = headers['Value']
                    cols = df.columns.tolist()
                    cols = cols[-2:] + cols[:-2]
                    df = df[cols]
                    return df

                elif stat_type.lower() == 'advanced':
                    df =  df['Advanced']
                    df['Split'] = headers['Split']
                    df['Value'] = headers['Value']
                    cols = df.columns.tolist()
                    cols = cols[-2:] + cols[:-2]
                    df = df[cols]
                    return df
                elif stat_type.lower() == 'totals':
                    df = df['Totals']
                    df['Split'] = headers['Split']
                    df['Value'] = headers['Value']
                    cols = df.columns.tolist()
                    cols = cols[-2:] + cols[:-2]
                    df = df[cols]
                    return df
            else:
                raise Exception('The "stat_type" you entered does not exist. The following options are: PER_GAME, SHOOTING, ADVANCED, TOTALS')
