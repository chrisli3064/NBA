from requests import Session as HttpSession
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import pandas as pd
import unicodedata, unidecode
from sys import stderr
import random
import string
import time
import re

class RetriableRequest:
    __session = None
    request_count = 0
    first_request_time = 0
    @staticmethod
    def __init__session__():
        if RetriableRequest.__session is not None:
            return
        print("Initializing HTTPS Session\n")
        RetriableRequest.__session = HttpSession()
        
        retries = Retry(total=0,
                        backoff_factor=1,
                        status_forcelist=[429])
        RetriableRequest.__session.mount("https://", HTTPAdapter(max_retries=retries))
        
    @staticmethod
    def get(url):
        RetriableRequest.__init__session__()

        current_time = time.time()
        # Check if the minute has changed since the first request in the last 60 seconds request
        if current_time - RetriableRequest.first_request_time > 60:
            # Reset the request count and update the last request time
            RetriableRequest.request_count = 0
        # Check if the request limit has been reached for this minute
        if RetriableRequest.request_count >= 14:
            # Calculate the remaining seconds since the first request of the last 60 seconds
            elapsed_time = current_time - RetriableRequest.first_request_time
            remaining_seconds = 70 - elapsed_time
            if remaining_seconds > 0:
                print(f"Request limit reached. Waiting a minute...")
                time.sleep(60)
            # Reset the request count and update the last request time after the wait
            RetriableRequest.request_count = 0
        # Make the request
        x = time.localtime(current_time)
        print(f"Request # {RetriableRequest.request_count} @ {x.tm_hour % 12}:{x.tm_min}:{x.tm_sec}")
        # Update the first request time if it is the first request of the last 60 seconds
        if RetriableRequest.request_count == 0:
            RetriableRequest.first_request_time = current_time
        time.sleep(random.uniform(0, 100)/100)
        response = RetriableRequest.__session.get(url)
        
        if response.status_code == 429:
            current_time = time.time()
            check_again_5min = time.localtime(current_time + 5 * 60)
            check_again_1hour = time.localtime(current_time + 60 * 60)
            check_again_1hour_minute = str(check_again_1hour.tm_min).zfill(2)
            
            message = f"Too many requests. Check again at {check_again_5min.tm_hour % 12}:{check_again_5min.tm_min} (in 5 minutes)"
            message += f" and again at {check_again_1hour.tm_hour % 12}:{check_again_1hour_minute} (in 1 hour)"
            
            raise ValueError(message)
        
        # print('REQUEST!')
        # Update the request count and last request time
        RetriableRequest.request_count += 1
        RetriableRequest.last_request_time = current_time
        return response


def get_game_suffix(date, team1, team2):
    r = RetriableRequest.get(f'https://www.basketball-reference.com/boxscores/index.fcgi?year={date.year}&month={date.month}&day={date.day}')
    suffix = None
    if r.status_code==200:
        soup = BeautifulSoup(r.content, 'html.parser')
        for table in soup.find_all('table', attrs={'class': 'teams'}):
            for anchor in table.find_all('a'):
                if 'boxscores' in anchor.attrs['href']:
                    if team1 in anchor.attrs['href'] or team2 in anchor.attrs['href']:
                        suffix = anchor.attrs['href']
    return suffix

"""
    Helper function for inplace creation of suffixes--necessary in order
    to fetch rookies and other players who aren't in the /players
    catalogue. Added functionality so that players with abbreviated names
    can still have a suffix created.
"""
def create_last_name_part_of_suffix(potential_last_names):
    last_names = ''.join(potential_last_names)
    if len(last_names) <=5:
        return last_names.lower()
    else:
        return last_names[:5].lower()

"""
    Amended version of the original suffix function--it now creates all
    suffixes in place.

    Since basketball reference standardizes URL codes, it is much more efficient
    to create them locally and compare names to the page results. The maximum
    amount of times a player code repeats is 5, but only 2 players have this
    problem--meaning most player URLs are correctly accessed within 1 to 2
    iterations of the while loop below.

    Added unidecode to make normalizing incoming string characters more
    consistent.

    This implementation dropped player lookup fail count from 306 to 35 to 0.
"""
def get_player_suffix(name,birthdate = None):
    normalized_name = unidecode.unidecode(unicodedata.normalize('NFD', name).encode('ascii', 'ignore').decode("utf-8"))
    if normalized_name == 'Metta World Peace' :
        return '/players/a/artesro01.html'
    elif normalized_name == 'Rick Hughes' :
        return '/players/h/hugheri02.html'
    elif normalized_name == 'Mahmoud Abdul-Rauf':
        return '/players/a/abdulma02.html'
    elif normalized_name =='Nene': 
        return  '/players/h/hilarne01.html'
    elif normalized_name == 'Sasha Pavlovic':
        return '/players/p/pavloal01.html'
    elif normalized_name == 'Mo Williams':
        return '/players/w/willima01.html'
    elif normalized_name == 'Marvin Williams':
        return '/players/w/willima02.html'
    elif normalized_name == 'J.J. Barea': 
        return '/players/b/bareajo01.html'
    elif normalized_name == 'Marcus Vinicius':
        return '/players/v/vincima01.html'
    elif normalized_name == 'Mouhamed Sene':
        return '/players/s/senesa01.html'
    elif normalized_name == 'Henry Walker':
        return '/players/w/walkebi01.html'
    elif normalized_name == 'Jeff Ayres':
        return '/players/p/pendeje02.html'
    elif normalized_name == 'Enes Freedom':
        return '/players/k/kanteen01.html'
    elif normalized_name == 'P.J. Hairston':
        return '/players/h/hairspj02.html'
    elif normalized_name == 'Clint Capela':
        return '/players/c/capelca01.html'
    elif normalized_name == 'Edy Tavares':
        return '/players/t/tavarwa01.html'
    elif normalized_name == 'Xavier Munford':
        return '/players/m/munfoxa02.html'
    elif normalized_name == 'Sheldon Mac':
        return '/players/m/mcclesh01.html'
    elif normalized_name == 'Cedi Osman':
        return '/players/o/osmande01.html'
    elif normalized_name == 'Maxi Kleber':
        return '/players/k/klebima01.html'
    elif normalized_name == 'Frank Ntilikina':
        return '/players/n/ntilila01.html'
    elif normalized_name == 'Didi Louzada':
        return '/players/l/louzama01.html'
    elif normalized_name == 'Gigi Datome':
        return '/players/d/datomlu01.html'
    else:
        split_normalized_name = normalized_name.split(' ')
        if len(split_normalized_name) < 2:
            return None
        initial = normalized_name.split(' ')[1][0].lower()
        name = name.translate(str.maketrans("", "", string.punctuation))
        all_names = name.split(' ')
        first_name_part = unidecode.unidecode(all_names[0][:2].lower())
        first_name = all_names[0]
        other_names = all_names[1:]
        other_names_search = other_names
        other_names =[name for name in other_names if (name!='II' and name!= 'III')]
        last_name_part = create_last_name_part_of_suffix(other_names)
        suffix = '/players/'+initial+'/'+last_name_part+first_name_part+'01.html'
        if suffix == '/players/w/willima01.html':
            suffix = '/players/w/willima02.html'
    # print(f'https://www.basketball-reference.com{suffix}')
    player_r = RetriableRequest.get(f'https://www.basketball-reference.com{suffix}')
    while player_r.status_code == 404:
        print(other_names_search)
        other_names_search.pop(0)
        last_name_part = create_last_name_part_of_suffix(other_names_search)
        initial = last_name_part[0].lower()
        suffix = '/players/'+initial+'/'+last_name_part+first_name_part+'01.html'
        player_r = RetriableRequest.get(f'https://www.basketball-reference.com{suffix}')
    while player_r.status_code==200:
        player_soup = BeautifulSoup(player_r.content, 'html.parser')
        h1 = player_soup.find('h1')

        if h1:
            page_name = h1.find('span').text
            foundName = False
            """
                Test if the URL we constructed matches the
                name of the player on that page; if it does,
                return suffix, if not add 1 to the numbering
                and recheck.
            """
            currentBirthday = None
            if birthdate:
                currentBirthday = player_soup.find(attrs={'id': 'necro-birth','data-birth':birthdate})
            if ( currentBirthday is not None if birthdate else True) or (((((unidecode.unidecode(page_name)).lower() == normalized_name.lower()) or foundName) and ( currentBirthday is not None if birthdate else True))):
                return suffix
            else:
                page_names = unidecode.unidecode(page_name).lower().split(' ')
                page_first_name = page_names[0]
                if first_name.lower() == page_first_name.lower() and currentBirthday is not None if birthdate else True:
                    return suffix
                # if players have same first two letters of last name then just
                # increment suffix
                elif first_name.lower()[:2] == page_first_name.lower()[:2]:
                    player_number = int(''.join(c for c in suffix if c.isdigit())) + 1
                    if player_number < 10:
                        player_number = f"0{str(player_number)}"
                    suffix = f"/players/{initial}/{last_name_part}{first_name_part}{player_number}.html"
                else:
                    other_names_search.pop(0)
                    last_name_part = create_last_name_part_of_suffix(other_names_search)
                    initial = last_name_part[0].lower()
                    suffix = '/players/'+initial+'/'+last_name_part+first_name_part+'01.html'
                
                player_r = RetriableRequest.get(f'https://www.basketball-reference.com{suffix}')
    if player_r.status_code != 200:
        print(player_r.status_code)
        print(player_r.content)

    return None


def remove_accents(name, team, season_end_year):
    alphabet = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXZY ')
    if len(set(name).difference(alphabet))==0:
        return name
    r = RetriableRequest.get(f'https://www.basketball-reference.com/teams/{team}/{season_end_year}.html')
    team_df = None
    best_match = name
    if r.status_code==200:
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('table')
        team_df = pd.read_html(str(table))[0]
        max_matches = 0
        for p in team_df['Player']:
            matches = sum(l1 == l2 for l1, l2 in zip(p, name))
            if matches>max_matches:
                max_matches = matches
                best_match = p
    return best_match
