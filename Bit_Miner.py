
import asyncio
from tqdm.asyncio import tqdm

from os import mkdir, system
from os.path import exists
system('color')

import time; import pytz
from datetime import datetime
import random; import json
from functools import partial
from termcolor import colored

from twitchAPI.helper import first
from twitchAPI.twitch import Twitch
from twitchAPI.oauth import UserAuthenticator
from twitchAPI.eventsub.webhook import EventSubWebhook
from twitchAPI.object.eventsub import StreamOnlineEvent, StreamOfflineEvent
from twitchAPI.type import AuthScope, ChatEvent
from twitchAPI.chat import Chat, EventData, ChatMessage, ChatSub, JoinedEvent, LeftEvent   
from twitchAPI.chat import JoinEvent,  MessageDeletedEvent, ClearChatEvent   # ChatCommand

from database_twitch import DBHelper, print_start_info, colors
import config

database = DBHelper(dbname="twitch_database.db")

#-------------------------------------------#
APP_ID = config.bot_client_id
APP_SECRET =  config.bot_client_secret

EVENTSUB_URL = config.bot_eventsub_url

USER_SCOPE = [AuthScope.CHAT_READ] 
#-------------------------------------------#

#----------------------------------------------------------------------------------#
with open('target_channels.json') as f:
    streamers = json.load(f)

# TARGET_CHANNELS = []
TARGET_CHANNELS = streamers.get("streamers")
#----------------------------------------------------------------------------------#

date_of_today = datetime.now().date()
streamers_file_data = {'active_streams' : {}, 'inactive_streams':{}}
                                                        
async def on_line(chat : Chat, data: StreamOnlineEvent):

    await asyncio.sleep(5.65)
    streams_info = await first(chat.twitch.get_streams(user_login=data.event.broadcaster_user_login))
    
    this_streamer_save_loc = streamers_file_data['inactive_streams'].pop(data.event.broadcaster_user_login)
    c = this_streamer_save_loc['color']

    streamers_file_data["active_streams"][data.event.broadcaster_user_login] = {'streamer.id': this_streamer_save_loc['streamer.id'],
                                                                                'streamer.login': data.event.broadcaster_user_login,
                                                                                "started_at": int(datetime.timestamp(data.event.started_at)),
                                                                                "ended_at": 0,
                                                                                "is_mature" : streams_info.is_mature,
                                                                                "language": streams_info.language,
                                                                                'live_id': streams_info.id,
                                                                                "viewer_count":streams_info.viewer_count,
                                                                                "game_name": streams_info.game_name,
                                                                                "title": streams_info.title,
                                                                                #"created_at": this_streamer_save_loc['created_at'],
                                                                                #"broadcaster_type": this_streamer_save_loc['broadcaster_type'],
                                                                                "color": this_streamer_save_loc['color']}
    
    database.add_online_streamers_to_history(streamers_file_data["active_streams"][data.event.broadcaster_user_login])
    
    time_moment = int(datetime.timestamp(data.event.started_at))
    time_moment = datetime.fromtimestamp(time_moment, tz=pytz.timezone('Europe/Rome')).strftime('%H:%M:%S')

    print(' {} | stream {} by {} -'.format(colored(time_moment,'light_green'),
                                    colored('started','light_green'),
                                    colored('@'+data.event.broadcaster_user_login, c)), end="")
    
    await chat.join_room(data.event.broadcaster_user_login) 

    total_streams = len(streamers_file_data["inactive_streams"])+len(streamers_file_data["active_streams"])
    print("→ {}/{} online".format(len(streamers_file_data["active_streams"]), 
                                  total_streams), end="\n")

async def off_line(chat : Chat, data : StreamOfflineEvent):

    end_time = int(time.time())
    time_moment = datetime.fromtimestamp(end_time, tz=pytz.timezone('Europe/Rome')).strftime('%H:%M:%S')

    try:
        c = streamers_file_data["active_streams"][data.event.broadcaster_user_login]['color']
    except KeyError:
        c = "green"
        print(f"KeyError: {data.event.broadcaster_user_login}")

    print(' {} | stream {} by {} - '.format(colored(time_moment,"light_green"),
                                     colored("ended", "light_green"),
                                     colored("@"+ data.event.broadcaster_user_login, c)), end= "")
    
    this_streamer_save_loc = streamers_file_data['active_streams'].pop(data.event.broadcaster_user_login)

    streamers_file_data["inactive_streams"][data.event.broadcaster_user_login] = {'streamer.id': this_streamer_save_loc['streamer.id'],
                                                                                  #"created_at": this_streamer_save_loc['created_at'],
                                                                                  #"broadcaster_type": this_streamer_save_loc['broadcaster_type'],
                                                                                  "color": this_streamer_save_loc['color']} 
    
    data_end_time = {"streamer_id": this_streamer_save_loc['streamer.id'], "ended_time": end_time}
    database.add_closing_time(data_end_time)
    await chat.leave_room(data.event.broadcaster_user_login)

# when the event READY is triggered, which will be on bot start
async def on_ready(ready_event: EventData):

    me = await first(ready_event.chat.twitch.get_users(logins=ready_event.chat.username))
    colored_me = colored("@"+me.display_name, "light_green", attrs=["bold"])
    
    print(' Logged in {} as {} \n'.format(colored("Twitch.tv", "light_magenta", attrs=["bold"]), colored_me))


#--------- Joined Event ---------#
#-----------------------------------------------------------------------#
async def on_bot_joined(joined_event: JoinedEvent):
    c = streamers_file_data["active_streams"][joined_event.room_name]['color']
    print(f' {colored("@"+joined_event.room_name, c)} ', end="")
#-----------------------------------------------------------------------#
                           
       
#--------------- Left Event --------------#
#----------------------------------------------------------------------------------------------------------------#
async def on_bot_left(left_event: LeftEvent):

    total_streams = len(streamers_file_data["inactive_streams"])+len(streamers_file_data["active_streams"])
    c = streamers_file_data["inactive_streams"][left_event.room_name]['color']

    print("room left {} → {}/{} online".format(colored('@'+left_event.room_name, c),
                                               len(streamers_file_data["active_streams"]), 
                                               total_streams), end="\n")
#----------------------------------------------------------------------------------------------------------------#




#------------Message Deleted -------------------#
#----------------------------------------------------------------------------------------------------------------#
async def on_delete_message(msg_deleted : MessageDeletedEvent):

    time = datetime.fromtimestamp(msg_deleted.sent_timestamp/1000, tz=pytz.timezone('Europe/Rome')).strftime('%H:%M:%S')
    c = streamers_file_data["active_streams"][msg_deleted.room.name]['color']
    colored_streamer = colored('@'+msg_deleted.room.name, c)

    print(" {} | {} by {} « {} » @{}".format(time, colored('deleted msg', 'light_red'), 
                                             colored_streamer,
                                             msg_deleted.message[:10],
                                             msg_deleted.user_name[:20]), end="\n")
#----------------------------------------------------------------------------------------------------------------#



#------------ On_user_ban ----------#
#----------------------------------------------------------------------------------------------------------------#
async def on_user_ban(ban_event : ClearChatEvent):
    
    moment_time = datetime.fromtimestamp(ban_event.sent_timestamp/1000, tz=pytz.timezone('Europe/Rome')).strftime('%H:%M:%S')
    event = colored('timed-out', 'red') if ban_event.duration else colored('banned', 'red')
    c = streamers_file_data["active_streams"][ban_event.room_name]['color']

    print(" {} | {} from {} @{} {}".format(moment_time, event,
                                        colored('@'+ban_event.room_name, c), 
                                        ban_event.user_name,
                                       "→ " +str(ban_event.duration)+ " sec" if ban_event.duration else ""), end="\n")
#----------------------------------------------------------------------------------------------------------------#
    

#------------ Raid Event ---------#
#----------------------------------------------------------------------------------------------------------------#
async def on_raid(raid_event: dict):

    raid_data = database.add_raids_to_database(raid_event)
    raid_data['time'] = datetime.fromtimestamp(raid_data['time'], tz=pytz.timezone('Europe/Rome')).strftime('%H:%M:%S')

    c = streamers_file_data["active_streams"][raid_data['streamer_name']]['color']

    print(" {} | {} for {} from @{}: {} viewers".format(raid_data['time'],
                                                        colored('raid event','light_magenta'),
                                                        colored('@'+ raid_data['streamer_name'], c),
                                                        raid_data['raider'], raid_data['viewers']), end="\n")
    
#----------------------------------------------------------------------------------------------------------------#

# msg in a channel send by either the bot OR another user
async def on_message(msg: ChatMessage): 
    
    if msg.bits > 0:

        bits_data = database.add_bits_to_database(msg)
        bits_data['time'] = datetime.fromtimestamp(bits_data['time'], tz=pytz.timezone('Europe/Rome')).strftime('%H:%M:%S')
        c = streamers_file_data["active_streams"][msg.room.name]['color']

        print(" {} | {} for {} from @{} - {} bits".format(bits_data['time'], colored('cheered bits', 'light_yellow'),
                                                          colored('@'+msg.room.name, c),
                                                          msg.user.name, msg.bits), end="\n")

# this will be called whenever someone subscribes to a channel
async def on_sub(sub: ChatSub):
    
    sub_data = database.add_subs_to_database(sub)
    time_moment = datetime.fromtimestamp(sub_data["time"], tz=pytz.timezone('Europe/Rome')).strftime('%H:%M:%S')
    c = streamers_file_data["active_streams"][sub.room.name]['color']

    print(' {} | {} for {} - {} - {} @{}'.format(time_moment, colored("subscription", "yellow"),
                                             colored("@"+sub.room.name, c), sub.sub_plan, 
                                             sub.sub_type, sub_data["subscriber"]), end="\n")
    

# --------------------- #
#  Setting up the Bot:  #
# --------------------- #
async def main():

    global streamers_file_data
    # set up twitch api instance and add user authentication with some scopes
    twitch = await Twitch(APP_ID, APP_SECRET)
    auth = UserAuthenticator(twitch, USER_SCOPE, force_verify=False)
    token, refresh_token = await auth.authenticate()
    await twitch.set_user_authentication(token, USER_SCOPE, refresh_token)

    eventsub = EventSubWebhook(EVENTSUB_URL, 9999, twitch)
    await eventsub.unsubscribe_all()
    eventsub.start()

    # create chat instance
    chat = await Chat(twitch)
    chat.register_event(ChatEvent.READY, on_ready)
    chat.start()
    await asyncio.sleep(2)

    offline_callback = partial(off_line, chat)
    online_callback = partial(on_line, chat)
                
    # >> Looking for live streamers:
    #print(' >> Looking for live streamers:')
    async for streamer in tqdm(twitch.get_users(logins=TARGET_CHANNELS), 
                               total=len(TARGET_CHANNELS), 
                               desc =' >> Looking for live streamers',
                               bar_format="{l_bar}{bar:30}{r_bar}{bar:-10b}"):
        
        streamer_data = (streamer.id, streamer.login, int(datetime.timestamp(streamer.created_at)), streamer.broadcaster_type)
        
        database.add_streamer_in_streamer_table(streamer_data)
        
        streams_info = await first(twitch.get_streams(user_login=streamer.login))

        if streams_info is not None:
            
            streamers_file_data["active_streams"][streamer.login] = {'streamer.id': streamer.id,
                                                                     'streamer.login': streamer.login,
                                                                     'started_at': int(datetime.timestamp(streams_info.started_at)),
                                                                     "ended_at": 0,
                                                                     'is_mature': streams_info.is_mature,
                                                                     'language': streams_info.language, 
                                                                     'live_id': streams_info.id,
                                                                     'viewer_count': str(streams_info.viewer_count),
                                                                     'game_name': streams_info.game_name,
                                                                     'title': streams_info.title,
                                                                     "broadcaster_type": streamer.broadcaster_type,
                                                                     "created_at": streamer.created_at}
            
            database.add_online_streamers_to_history(streamers_file_data["active_streams"][streamer.login])
            await eventsub.listen_stream_offline(streamer.id,  offline_callback)
            # await eventsub.listen_stream_online(streamer.id,  online_callback)
            
        else:
            streamers_file_data["inactive_streams"][streamer.login] = {"streamer.id": streamer.id,
                                                                       #"created_at": streamer.created_at,
                                                                       #"broadcaster_type": streamer.broadcaster_type,
                                                                       "color": random.choice(colors)}

    await asyncio.sleep(2)                                                            
    streamers_file_data = print_start_info(streamers_file_data, colors)

    # register the handlers for the events:
    # listen to when the bot is done starting up and ready to join channel    
    chat.register_event(ChatEvent.JOINED,  on_bot_joined)
    
    print("\n >> joining Channels:", end="")
    
    await chat.join_room(list(streamers_file_data['active_streams'].keys()))                   
    print("\n", end="\n"); await asyncio.sleep(0.25)  

    chat.register_event(ChatEvent.MESSAGE, on_message)          # listen to chat messages
    chat.register_event(ChatEvent.SUB, on_sub)                  # listen to channel subscriptions
    chat.register_event(ChatEvent.RAID, on_raid)                # listen to channel raids
    chat.register_event(ChatEvent.CHAT_CLEARED, on_user_ban)    # listen to channel ban or suspension
    chat.register_event(ChatEvent.MESSAGE_DELETE, on_delete_message)
    chat.register_event(ChatEvent.LEFT, on_bot_left)
    # chat.register_event(ChatEvent.USER_LEFT, on_left)
    # chat.register_command('reply', test_command)

    now = datetime.now()

    copy = streamers_file_data.copy()
    for k, group in copy.items():
        group_copy = group.copy()
        for streamer_info in group_copy.values():
            if k == "active_streams":
                await eventsub.listen_stream_online(streamer_info['streamer.id'],  online_callback)
            else:
                await eventsub.listen_stream_online(streamer_info['streamer.id'],  online_callback)
                await eventsub.listen_stream_offline(streamer_info['streamer.id'],  offline_callback)
    
    tot_time = str(datetime.now() - now)[3] +"min "+ str(datetime.now() - now)[5:7] + "sec"
                
    print(" {} | {} alerts ready → {}".format(colored(datetime.now().time().strftime('%H:%M:%S'), 'light_green'),
                                              colored('off/on-line', "light_green", attrs=["bold"]),
     
                                              colored(tot_time, 'light_green')))
    
    while True:
        await asyncio.sleep(230)
        copy_active_streams = streamers_file_data['active_streams'].copy()
        task = asyncio.create_task(my_coroutine(copy_active_streams, twitch))
        updates = await task
        database.update_streamer_Streams(updates)
        
        #try: input()
        #finally:
        #        print("Exiting program...")
        #        task.cancel()
        #        print("task cancelled")
        #        await eventsub.stop()
        #        print("eventsub.stop")
        #        chat.stop()          # close the chat bot
        #        print("chat_stop")
        #        await twitch.close() # close twitch api client 
        #        print("chat_stop")
#
        #print(" >> Clearning files...\n", end =" ")
        #break
        

    #run till we press enter in the console    
    #try: input()
    #finally:

# ------------------------------------------------------------------------------------------------------- # 
# ------------------------------------------------------------------------------------------------------- # 
async def my_coroutine(active_stream_dict, twitch):

    data = {}
    copia = active_stream_dict.copy()

    for streamer in copia.keys():

        streams_info =  await first(twitch.get_streams(user_login=streamer))

        if streams_info is not None:

            data[streams_info.user_login] = {"game_name": streams_info.game_name, 
                                             "live_id" : streams_info.id,
                                             "title": streams_info.title, 
                                             "viewers": streams_info.viewer_count, 
                                             "started_at": int(datetime.timestamp(streams_info.started_at)),
                                             "now": int(datetime.now().timestamp()),
                                             "color": active_stream_dict[streamer]["color"]}
    
    return data
# ------------------------------------------------------------------------------------------------------- # 
# ------------------------------------------------------------------------------------------------------- # 

if __name__ == "__main__":
    
    print(''' 
    _______       _ _       _        ____        _   
   |__   __|     (_) |     | |      |  _ \      | |  
      | |_      ___| |_ ___| |__    | |_) | ___ | |_ 
      | \ \ /\ / / | __/ __| '_ \___|  _ < / _ \| __|
      | |\ V  V /| | || (__| | | |__| |_) | (_) | |_ 
      |_| \_/\_/ |_|\__\___|_| |_|  |____/ \___/ \__| 
          
   press ENTER to stop \n''')
    
    asyncio.run(main())
