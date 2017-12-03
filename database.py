# necessary imports
from __future__ import division
import requests
import sqlite3

# We created our database in the Pycharm IDE
# establish connection to our database
conn = sqlite3.connect('dota.db')
conn.text_factory = str
c = conn.cursor()

# This function inserts new rows to our match_details table
def add_match_details(match_details_data):
    try:
        # Construct the query format
        query = ''' INSERT INTO match_details(match_id,player_id,is_radiant)
                VALUES(?,?,?)'''
        # fill in the query for a whole array of tuples with (match_id, player_id, is_radiant) values and execute it
        # thus, inserting multiple rows at once
        c.executemany(query, match_details_data)
    finally:
        return

# This function inserts a new row to our chat table
def add_chat(chat_data):
    try:
        query = ''' INSERT INTO chat(match_id, player_id, message, time)
                VALUES(?,?,?,?)'''
        c.executemany(query, chat_data)
    finally:
        return

# This function receives a playerID, performs a request for further information,
# selects the relevant information and inserts it to the player table
def add_player(player_id, is_pro=False):
    response = requests.get('https://api.opendota.com/api/players/' + str(player_id)).json()
    try:
        # get only the relevant information
        profile = response['profile']
        name = profile['personaname']
        country = profile['loccountrycode']
        mmr = response['mmr_estimate']['estimate']

        query = ''' INSERT INTO players(id, name, mmr, country, is_pro)
                VALUES(?,?,?,?,?)'''
        # the 'execute' function inserts only one row compared to executemany
        c.execute(query, [player_id, name, mmr, country, is_pro])
    finally:
        return

# This function inserts a new row to our matches table
def add_match(match_id, radiant_won, is_pro):
    try:
        query = ''' INSERT INTO matches(id, radiant_won, is_pro)
                VALUES(?,?,?)'''
        c.execute(query, [match_id, radiant_won, is_pro])
    finally:
        return

# This function has an input of a matchID
def process_match(match_id, is_pro=False):
    # perform a request to get all the details of a specific game
    response = requests.get('https://api.opendota.com/api/matches/' + str(match_id)).json()
    radiant_win = response['radiant_win']
    players = {}

    # add this match to our matches table
    add_match(match_id, radiant_win, is_pro)

    match_details_data = []
    # for every element in the response['players'] list check if has an account_id key
    # every player without an account id is filtered out (since players can deny their account id to be shown).
    # players get a player_slot in each match, and they are referenced to through this value (eg. during chat), so we also need a 
    # dictionary to be able to look up which player_slot is which account.
    for player in filter(lambda player: player['account_id'], response['players']):
        # save the player to the database
        player_id = player['account_id']
        add_player(player_id, is_pro)
        # insert the player id into a dictionary, we will need that later
        players[player['player_slot']] = player_id
        # since every player is added to the match_details table at the same time (for performance optimization purposes), 
        # we make an array of all the players present in a specific match, and then later feed the whole array to the table.
        match_details_data.append((match_id, player_id, player['isRadiant']))

    # we use this table to connect the matches to the players and to store which side the player played during that game
    add_match_details(match_details_data)

    # the chat field in the api is only available for pro matches
    if response['chat']:
        chat_data = []
        # the input of the filter function is the response['chat'] list, containing dictionaries
        # for every element in the list, check the type is 'chat', this is necessary because dota has easily reachable build 
        # in text messages and we are not interested in them when working with text
        # the last criteria we check here is that the sender is defined in dictionary containing the infos about each msg    
        for chat_item in filter(lambda chat_item: chat_item['type'] == 'chat' and 'player_slot' in chat_item.keys(),
                                response['chat']):
            # if the sender is identified
            if chat_item['player_slot'] in players.keys():
                # save the message and some other relevant information (sender, timestamp and matchID)
                chat_data.append((match_id, players[chat_item['player_slot']], chat_item['key'], chat_item['time']))
        # When the whole game is processed from the chat point of view, save the data in the chat table
        add_chat(chat_data)

# this function is a bundler function, it basically get x matches that a player was involved in, and then processes all those matches
# We implemented this function, because all three of us played Dota earlier and we were curious abut our data also :)
def process_player(player_id, max_number_of_matches=100):
    sum_match = 0
    # get the player's matches
    response = requests.get('https://api.opendota.com/api/players/' + str(player_id) + '/matches').json()
    # work only with the last 100 games
    if len(response) > max_number_of_matches > 0:
        response = response[:max_number_of_matches]

    # just a simple variable for a simple 'progress bar' we use to monitor our progress, because the API we used was a bit slow
    length = len(response)
    # for every match returned by the API call call the process match function defined earlier
    for i, match in enumerate(response, start=1):
        print '\rFetching in progress... {0:.2f}%'.format(i / length * 100),
        try:
            process_match(match['match_id'])
            sum_match += 1
        except:
            pass

    print '\nA total of {} matches was added.'.format(sum_match)

# The other main function, it is used to gather pro matches, the input indicates how many API calls we want to perform
# one call will give us 100 matches.
def process_pro_matches(rounds):
    sum_match = 0
    url = 'https://api.opendota.com/api/proMatches'
    last_id = ''

    for i in range(rounds):
        # perform the request, get 100 matches and save the id of the last match,
        # so we can use it later for the next API call
        response = sorted([str(match['match_id']) for match in requests.get(url + last_id).json()])
        # the more recent a match is the bigger number it gets as a match_is. Thus, we make an ordered list to get the smallest
        # number easily. This match_id will be used in the next request, which will return another 100 matches that tooke place before
        # this given match_id
        last_id = '?less_than_match_id=' + response[0]

        #iterate through the results, and call the process match for every item
        for index, match_id in enumerate(response, start=1):
            print '\rFetching in progress... {0:.2f}%'.format((i + index / 100) / rounds * 100),
            try:
                process_match(match_id, is_pro=True)
                sum_match += 1
            except:
                pass

    print '\nA total of {} matches was added.'.format(sum_match)


# =========================================================================================

print 'Fetching started...\n'

# Get x pages of pro matches - expect that due to the slowness of the server around 1/10 matches will NOT be
# processed due to timeout error. One page contains 100 matches
process_pro_matches(1)

# Get matches of a particular player - same error can happen here. My profile contains 1062 matches - calculate
# with that in mind
# process_player(44030905)

#save the changes in the database
conn.commit()

print '\n...done!'


# =========================================================================================
