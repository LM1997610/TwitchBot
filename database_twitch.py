

import time
from datetime import datetime
import random; import json
from termcolor import colored

import sqlite3

colors = ["magenta", "light_magenta", "light_cyan", "cyan" , "blue", "dark_grey", "light_grey", "light_blue", "white"]

# ---------------------------------------------------
with open('target_channels.json') as f:
    streamers = json.load(f)

# TARGET_CHANNELS = []
TARGET_CHANNELS = streamers.get("streamers")
# ---------------------------------------------------


class DBHelper:

    def __init__(self, dbname="new_database.db"):
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname, check_same_thread=False)
        self.cur = self.conn.cursor()
        self.setup()

    def setup(self):
        stmt_streamer = '''CREATE TABLE IF NOT EXISTS Streamer_Table (streamer_id INTEGER PRIMARY KEY,
                                                                      streamer_name TEXT NOT NULL,
                                                                      created_at DATE NOT NULL,
                                                                      broadcaster_type TEXT NOT NULL)'''
        self.cur.execute(stmt_streamer)

        stmt_history = '''CREATE TABLE IF NOT EXISTS Stream_History (streamer_id INTEGER,
                                                                     streamer_name TEXT NOT NULL,
                                                                     started_at DATE NOT NULL,
                                                                     ended_at DATE,
                                                                     is_mature BOOLEAN,
                                                                     language TEXT NOT NULL,
                                                                     live_id INTEGER NOT NULL,
                                                                     start_viewer_count TEXT,
                                                                     PRIMARY KEY (streamer_id, started_at))'''

        self.cur.execute(stmt_history)                                     


        stmt_raids = '''CREATE TABLE IF NOT EXISTS Raids_Table (streamer_id INTEGER,
                                                                streamer_name TEXT NOT NULL,
                                                                user TEXT NOT NULL,
                                                                viewers INTEGER,
                                                                time DATE NOT NULL)'''  
        self.cur.execute(stmt_raids)   


    def add_streamer_in_streamer_table(self, streamer_data):

      stmt =f'''INSERT OR IGNORE INTO Streamer_Table (streamer_id, streamer_name, created_at, broadcaster_type) 
                                              VALUES {streamer_data}'''
      self.conn.execute(stmt)
      self.conn.commit()

    def add_online_streamers_to_history(self, stream_data):
        
        stmt_subs = f'''CREATE TABLE IF NOT EXISTS {stream_data['streamer.login'].lower()}_Subs (streamer_name TEXT NOT NULL,
                                                                                                 time DATE NOT NULL, 
                                                                                                 user TEXT NOT NULL,
                                                                                                 sub_plan TEXT NOT NULL,
                                                                                                 sub_type TEXT NOT NULL,
                                                                                                 kind_of_sub TEXT NOT NULL,
                                                                                                 message TEXT,
                                                                                                 info TEXT)'''
        self.cur.execute(stmt_subs)

        stmt_bits = f'''CREATE TABLE IF NOT EXISTS {stream_data['streamer.login'].lower()}_Bits (streamer_name TEXT NOT NULL,
                                                                                                 streamer_id INTEGER,
                                                                                                 time DATE NOT NULL, 
                                                                                                 user TEXT NOT NULL,
                                                                                                 bits INTEGER,
                                                                                                 is_mod BOOLEAN,
                                                                                                 is_vip BOOLEAN,
                                                                                                 is_sub BOOLEAN,
                                                                                                 message TEXT,
                                                                                                 badgeinfo TEXT)'''
        self.cur.execute(stmt_bits) 

        stmt_details = f'''CREATE TABLE IF NOT EXISTS {stream_data['streamer.login'].lower()}_Streams (live_id INTEGER NOT NULL,
                                                                                                       time_of_update INTEGER NOT NULL,
                                                                                                       type_of_update TEXT NOT NULL,
                                                                                                       new_update TEXT NOT NULL)''' 
        self.cur.execute(stmt_details) 

        tuple_of_data = tuple(stream_data.values())[:8]

        stmt = '''SELECT started_at FROM Stream_History WHERE streamer_id=? ORDER BY started_at DESC LIMIT 1'''
        self.cur.execute(stmt, (tuple_of_data[0], ))    
        last_started_at = self.cur.fetchone()   

        if last_started_at is None or last_started_at[0] != tuple_of_data[2]:
            stmt = f'''INSERT INTO Stream_History VALUES {tuple_of_data}'''
            self.conn.execute(stmt)
            self.conn.commit()


            for item in ["title", "game_name"]:

                stmt_streams = f'''INSERT INTO {stream_data['streamer.login'].lower()}_Streams VALUES {(stream_data["live_id"], 
                                                                                                        stream_data["started_at"], 
                                                                                                        item, 
                                                                                                        stream_data[item])}'''
                self.conn.execute(stmt_streams)
                self.conn.commit()

    def add_subs_to_database(self, data_from_twitch):
       
        new_data = { "streamer_name" : data_from_twitch.room.name,
                     "time" : int(time.time()),
                     "subscriber" : data_from_twitch.system_message.split('\\')[0],                # subscribed with Prime
                     "sub_plan": data_from_twitch.sub_plan,  # Prime, 1000, 2000             # subscribed at Tier 1 
                     "type" : data_from_twitch.sub_type,     # sub. resub, subgift           # gifted a Tier 1 sub to some_user!                       
                     "kind_of_sub" : " ".join(data_from_twitch.system_message.split(".")[0].split("\\s")[1:]), 
                     "message" : data_from_twitch.sub_message if data_from_twitch.sub_message else 0,
                     "info": 0}
        
        if  data_from_twitch.sub_type == 'resub':
            new_data["info"] =  data_from_twitch.system_message.split(".")[1].replace("\\s", " ").lstrip()

        elif data_from_twitch.sub_type == 'subgift':
             split = new_data["kind_of_sub"].rsplit('! ')
             new_data["kind_of_sub"] = split[0]
             new_data["info"] = " ".join(split[1:]) if len(split) > 1 else 0

        sub_data = tuple(new_data.values())
        stmt =f'''INSERT INTO {new_data["streamer_name"]}_Subs  VALUES {sub_data}'''
        try:
            self.conn.execute(stmt)
            self.conn.commit()
        except sqlite3.OperationalError:
            t =  datetime.now()
            print(f" {str(t.time())[:8]} | [Warning] lost sub for @{new_data['streamer_name']}!")

        return new_data
    
    def add_bits_to_database(self, data_from_twitch):
       
        new_data = { 'streamer_name' : data_from_twitch.room.name,
                     'streamer_id' : data_from_twitch.room.room_id,
                     'time' : int(data_from_twitch.sent_timestamp/1000),
                     'author.name' : data_from_twitch.user.name,
                     'bits' : data_from_twitch.bits,
                     'mod' : data_from_twitch.user.mod,
                     #'user-id' : data_from_twitch.user.id,
                     #'badges' : data_from_twitch.user.badges if data_from_twitch.user.badges else None,
                     #'emotes' : data_from_twitch.emotes if data_from_twitch.emotes else None, 
                     'vip': data_from_twitch.user.vip, 
                     'is_sub' : data_from_twitch.user.subscriber, 
                     'content' : data_from_twitch.text, 
                     'badge-info' : str(data_from_twitch.user.badge_info) if data_from_twitch.user.badge_info else 0}

        bits_data = tuple(new_data.values())
        stmt =f'''INSERT INTO  {new_data["streamer_name"]}_Bits VALUES {bits_data}'''
        try:
            self.conn.execute(stmt)
            self.conn.commit()
        except sqlite3.OperationalError:
            t =  datetime.now()
            print(f" {str(t.time())[:8]} | [Warning] lost bits for @{new_data['streamer_name']}!")

        return new_data
    
    def add_raids_to_database(self, raid_data_from_twitch):

        new_data = {'streamer_id': raid_data_from_twitch['tags']['room-id'], 
                    'streamer_name': raid_data_from_twitch['command']['channel'][1:],
                    'raider': raid_data_from_twitch['tags']['login'],
                    'viewers': raid_data_from_twitch['tags']['msg-param-viewerCount'], 
                    "time": int(int(raid_data_from_twitch['tags']['tmi-sent-ts'])/1000)}
        
        raids_data = tuple(new_data.values())
        stmt =f'''INSERT INTO  Raids_Table  VALUES {raids_data}'''
        self.conn.execute(stmt)
        self.conn.commit()
        return new_data
    

    def update_streamer_Streams(self, news_on_active_channels):
        check = True

        for key,values in news_on_active_channels.items():

            old_game = self.get_item_from_streamer_Streams(key, "game_name")
            old_title = self.get_item_from_streamer_Streams(key, "title")

            now = datetime.now()

            if old_title != values["title"]:

                update_stmt = f'''INSERT INTO {key.lower()}_Streams VALUES {(values["live_id"], 
                                                                             values["now"], "title", 
                                                                             values["title"])}'''
                
                sentence = " {} | changed {} for {}".format(str(now.time())[:8], 
                                                            colored('title', "light_green", attrs=["bold"]), 
                                                            colored('@'+key, values['color']))

                try:
                    self.conn.execute(update_stmt)
                    self.conn.commit()
                    check = False
                    print(sentence)

                except sqlite3.OperationalError:
                    print(f" {str(now.time())[:8]} | [Warning] new title for @{key} not saved !")
            
            if old_game != values["game_name"]:
                
                update_stmt = f'''INSERT INTO {key.lower()}_Streams VALUES {(values["live_id"], 
                                                                             values["now"], "game_name", 
                                                                             values["game_name"])}'''
                
                sentence = " {} | changed {} for {} in {}".format(str(now.time())[:8], 
                                                                  colored('category', "light_green", attrs=["bold"]), 
                                                                  colored("@"+key, values["color"]), values["game_name"])
                try:
                    self.conn.execute(update_stmt)
                    self.conn.commit()
                    check = False
                    print(sentence)

                except sqlite3.OperationalError:
                    print(f" {str(now.time())[:8]} | [Warning] new game for @{key} not saved !")

            selct_stmt = f'''SELECT streamer_name, start_viewer_count
                             FROM Stream_History WHERE streamer_name = "{key}" AND started_at = {values["started_at"]}'''
            
            self.cur.execute(selct_stmt)
            
            name, views = self.cur.fetchone() 
            new_viewer_count = views + "_" + str(values["viewers"])
            
            close_stmt = f'''UPDATE Stream_History set start_viewer_count = "{new_viewer_count}" 
                             WHERE streamer_name = "{key}" AND started_at = {values["started_at"]}'''
            
            self.conn.execute(close_stmt)
            self.conn.commit()
                
        if check == True: print(f" {str(now.time())[:8]} | system updating routine completed")

        
    def add_closing_time(self, data_end_time):

        stmt = '''SELECT started_at, ended_at, streamer_name FROM Stream_History WHERE streamer_id={} ORDER BY started_at DESC LIMIT 1'''.format(data_end_time["streamer_id"])

        self.cur.execute(stmt)

        last_started_time = self.cur.fetchone()
        
        if last_started_time[1] == 0:
            close_stmt = '''UPDATE Stream_History 
                            SET ended_at = {}
                            WHERE streamer_id = {} AND started_at = {}'''.format(data_end_time["ended_time"], 
                                                                                 data_end_time["streamer_id"], 
                                                                                 last_started_time[0])
            self.conn.execute(close_stmt)
            self.conn.commit()

        streamer_name = last_started_time[2]

        for table in ["Subs", "Bits"]:
            count_stmt = f'''SELECT count(*) FROM {streamer_name}_{table}'''
            self.cur.execute(count_stmt)
            count = self.cur.fetchone()

            if count[0] == 0:
                drop_stmt = f'''DROP TABLE IF EXISTS {streamer_name}_{table}'''
                self.cur.execute(drop_stmt)

    def get_item_from_streamer_Streams(self, streamer_name, type):

        stmt = f'''SELECT new_update FROM {streamer_name}_Streams 
                   WHERE type_of_update = "{type}" ORDER BY time_of_update DESC LIMIT 1'''
        
        self.cur.execute(stmt)
        results = self.cur.fetchone()
        return results[0]


def print_start_info(streamers_file_data : dict, colors):

    active_streams_dict = streamers_file_data['active_streams']
    print()
    for i, (streamer_name, values) in enumerate(active_streams_dict.items()):

        streamers_file_data['active_streams'][streamer_name]['color'] = colors[i] if i < len(colors) else random.choice(colors)
        c = streamers_file_data["active_streams"][streamer_name]['color']

        print(f" id: {values['streamer.id']:>9} |", end="")
        print(" {} with {} spects - {} | {} | {}".format(colored('@'+ streamer_name, c),
                                                            values['viewer_count'],
                                                            values['game_name'][:25],
                                                            values['created_at'].date().strftime('%Y'),
                                                            values['broadcaster_type']), end="\n" )
        values.pop('created_at')
        values.pop('broadcaster_type')
    
    tot_streams = sum([len(streamers_file_data[key]) for key in streamers_file_data.keys()])
    print()
    print(f" >> Live Channels → {len(streamers_file_data['active_streams'].keys())} out of {tot_streams}", end="\n")

    return streamers_file_data



