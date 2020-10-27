import time
import numpy as np
from requests.exceptions import HTTPError
import pandas as pd
import riotwatcher
import json
import sqlite3

api_key = 'REDACTED'

watcher = riotwatcher.LolWatcher(api_key)
conn_na = sqlite3.connect('F:/Coding/Data/League Data/soloq_na.db')
conn_euw = sqlite3.connect('F:/Coding/Data/League Data/soloq_euw.db')
conn_kr = sqlite3.connect('F:/Coding/Data/League Data/soloq_kr.db')

conn_dict = {'na1': conn_na,
             'euw1': conn_euw,
             'kr': conn_kr}

# TODO: Item dicts are either outdated now or are going to be very soon.

with open('F:/Coding/Data/League Data/champion.json', encoding='utf8') as champion_file:
    champion_data = json.load(champion_file)
df_champs = pd.DataFrame(champion_data['data'])
champion_dict = pd.Series(df_champs.loc['id'].values, index=df_champs.loc['key'].astype('int64'))
champion_dict['-1'] = 'None'

with open('F:/Coding/Data/League Data/item.json', encoding='utf8') as item_file:
    item_data = json.load(item_file)
item_dict = pd.DataFrame(item_data['data']).loc['name']
item_dict['0'] = 'None'
item_dict.index = item_dict.index.map(int)

with open('F:/Coding/Data/League Data/summoner.json', encoding='utf8') as summ_file:
    summ_data = json.load(summ_file)
df_summ = pd.DataFrame(summ_data['data'])
summ_dict = pd.Series(df_summ.loc['name'].values, index=df_summ.loc['key'].astype('int64'))


# Helper function to be applied in a lambda statement in the following function
def get_account_id(summonerId, region):
    return watcher.summoner.by_id(region=region, encrypted_summoner_id=summonerId)['accountId']


# Gets all account id's and sets it up in a data frame
def get_challenger_accounts(region):
    df_current = pd.DataFrame(watcher.league.challenger_by_queue(region=region, queue='RANKED_SOLO_5x5')['entries'])
    df_current['accountId'] = df_current['summonerId'].apply(lambda x: get_account_id(x, region=region))
    return df_current


# Delivers list of game ID's when given a list of accounts
def populate_game_list(region, accounts):
    game_list = []
    for i in range(len(accounts['accountId'])):
        try:
            current_history = watcher.match.matchlist_by_account(region=region,
                                                                 encrypted_account_id=accounts['accountId'][i],
                                                                 queue=420)
        except HTTPError as err:
            print('Found HTTPError')
            time.sleep(123)
            current_history = watcher.match.matchlist_by_account(region=region,
                                                                 encrypted_account_id=accounts['accountId'][i],
                                                                 queue=420)
        current_match_list = pd.DataFrame(current_history['matches'])['gameId'].tolist()
        game_list = game_list + current_match_list
    db_list = pd.read_sql_query('SELECT gameId FROM match', con=conn_dict[region])
    final_list = [x for x in game_list if x not in db_list['gameId'].tolist()]
    return set(final_list)


def get_games(region, game_list):
    master_list = []
    for i in game_list:
        while True:
            try:
                master_list.append(watcher.match.by_id(region=region, match_id=i))
                if len(master_list) % 1000 == 0:
                    print(len(master_list))
            except HTTPError:
                print('Found HTTPError')
                time.sleep(123)
                master_list.append(watcher.match.by_id(region=region, match_id=i))
                if len(master_list) % 1000 == 0:
                    print(len(master_list))
            else:
                break
    return master_list


def challenger_games_wrapper(region):
    df_challenger = get_challenger_accounts(region=region)
    current_game_list = populate_game_list(region=region, accounts=df_challenger)
    print(str(len(current_game_list))+' games to be added.')
    return get_games(region=region, game_list=current_game_list)


def games_to_sql(master_list, conn):
    match_columns = ['gameId', 'platformId', 'gameCreation', 'gameDuration', 'queueId',
                     'mapId', 'seasonId', 'gameVersion', 'gameMode', 'gameType']
    df_match = pd.DataFrame([pd.Series(master_list[i])[match_columns] for i in range(len(master_list))])

    df_teams = pd.concat([pd.DataFrame(master_list[i]['teams']) for i in range(len(master_list))], ignore_index=True)

    df_teams = df_teams.drop('bans', axis=1).drop('dominionVictoryScore', axis=1).drop('vilemawKills', axis=1)
    df_teams['gameId'] = np.repeat(df_match['gameId'].values, 2)

    df_bans1 = pd.concat([pd.DataFrame(master_list[0]['teams'][0]['bans']) for i in range(len(master_list))],
                         ignore_index=True)
    df_bans2 = pd.concat([pd.DataFrame(master_list[1]['teams'][1]['bans']) for i in range(len(master_list))],
                         ignore_index=True)
    df_bans = pd.concat([df_bans1, df_bans2])

    df_bans['gameId'] = np.tile(np.repeat(df_match['gameId'].values, 5), 2)
    df_bans['championName'] = df_bans['championId'].map(champion_dict)

    participants_columns = ['participantId', 'teamId', 'championId', 'spell1Id', 'spell2Id']
    df_participants = pd.concat(
        [pd.DataFrame(master_list[i]['participants'])[participants_columns] for i in range(len(master_list))],
        ignore_index=True)

    df_participants['championName'] = df_participants['championId'].map(champion_dict)
    df_participants['spell1Name'] = df_participants['spell1Id'].map(summ_dict)
    df_participants['spell2Name'] = df_participants['spell2Id'].map(summ_dict)
    df_participants['gameId'] = np.repeat(df_match['gameId'].values, 10)

    df_participants_stats = pd.concat(
        [pd.json_normalize(master_list[i]['participants']) for i in range(len(master_list))],
        sort=False)

    df_participants_stats['gameId'] = np.repeat(df_match['gameId'].values, 10)
    df_participants_stats['championName'] = df_participants_stats['championId'].map(champion_dict)

    df_participants_stats['item0'] = df_participants_stats['stats.item0'].map(item_dict)
    df_participants_stats['item1'] = df_participants_stats['stats.item1'].map(item_dict)
    df_participants_stats['item2'] = df_participants_stats['stats.item2'].map(item_dict)
    df_participants_stats['item3'] = df_participants_stats['stats.item3'].map(item_dict)
    df_participants_stats['item4'] = df_participants_stats['stats.item4'].map(item_dict)
    df_participants_stats['item5'] = df_participants_stats['stats.item5'].map(item_dict)
    df_participants_stats['item6'] = df_participants_stats['stats.item6'].map(item_dict)

    df_participants_identities = pd.concat(
        [pd.json_normalize(master_list[i]['participantIdentities']) for i in range(len(master_list))])

    df_participants_identities['gameId'] = np.repeat(df_match['gameId'].values, 10)

    df_match.to_sql('match', con=conn, if_exists='append')
    df_teams.to_sql('teams', con=conn, if_exists='append')
    df_bans.to_sql('bans', con=conn, if_exists='append')
    df_participants.to_sql('participants', con=conn, if_exists='append')
    df_participants_stats.to_sql('participants_stats', con=conn, if_exists='append', chunksize=1000)
    df_participants_identities.to_sql('participants_identities', con=conn, if_exists='append', chunksize=1000)


current_region = 'na1'
games_to_sql(challenger_games_wrapper(region=current_region), conn=conn_dict[current_region])
