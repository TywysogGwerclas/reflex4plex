# def create_connection(db_file):
#     """ create a database connection to the SQLite database
#         specified by the db_file
#     :param db_file: database file
#     :return: Connection object or None
#     """
#     conn = None
#     try:
#         conn = sqlite3.connect(db_file)
#     except Error as e:
#         print(e)
#
#     return conn
#
import sqlite3
from time import sleep
from shutil import copyfile
from os import path, listdir, remove, rename
from datetime import datetime
from sys import platform
import configparser
from pathlib import Path as pathlib

# # Config files #
# playlists = ["2111"]   NEEDS TO BE A DICT
# threshold = 60000 # How far before burning or recycling first episode in milliseconds. plex doesn't record anything before 60000 for tv and movies.
# polling = 10 # How Often to check the database for changes in seconds
# burn = False # True: Remove the first episode. False: Send it to the back.
# timeout = 300
#
# stall = 0
# stall_threshold = 60
# db_location = ""





# Backup
def db_backup(backup_level, db_location):


    today = datetime.now()
    backup_path= 'backup/backup.{}.db'.format(str(today.strftime("%d-%m-%y")))
    print(backup_path)
    pathlib(backup_path).touch()

    copyfile(db_location, backup_path)
    print("Created database backup.")
    backup_list = listdir("backup/")
    if len(backup_list) > backup_level:
        remove("backup/"+backup_list[0])
        print("Removed oldest backup.")
    return

# read Loop
def db_connect(db_location):
    db = None

    try:
        db = sqlite3.connect(db_location)
    except Error as e:
        print(e)
    cursor = db.cursor()
    return db, cursor


def playlist_reset(db, cursor, playlist_id):

    cursor.execute('SELECT MIN("order") FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}"'.format(playlist_id))
    dif = 0
    try:
        dif = int(cursor.fetchone()[0]) - 1000
    except:
        pass


    if dif > 0:
        cursor.execute('''UPDATE play_queue_generators SET "order" = "order" - {}
        WHERE play_queue_generators.playlist_id = "{}"'''.format(dif, playlist_id))
        db.commit()
    return



def poll(db, cursor, playlist_id, burn, threshold):
    watching = ""
    try:
        cursor.execute('SELECT metadata_item_id FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}" AND play_queue_generators."order" = 1000.0'.format(playlist_id))
        watching = cursor.fetchone()
        watching[0]
    except:
        print('Playlist {} has been rearraged. Resetting order ids.'.format(playlist_id))
        playlist_reset(db, cursor, playlist_id)
        cursor.execute('SELECT metadata_item_id FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}" AND play_queue_generators."order" = 1000.0'.format(playlist_id))
        watching = cursor.fetchone()

    cursor.execute('SELECT guid, title FROM metadata_items WHERE metadata_items.id = "{}"'.format(watching[0]))
    meta = cursor.fetchone()

    cursor.execute('''SELECT view_offset FROM metadata_item_settings WHERE metadata_item_settings.guid = "{}"'''.format(meta[0]))
    offset = cursor.fetchone()
    if not offset:
        print("Waiting.")
        return 0
    if not offset[0]:
        cursor.execute('SELECT metadata_item_id FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}" AND play_queue_generators."order" = 2000.0'.format(playlist_id))
        ondeck = cursor.fetchone()

        cursor.execute('SELECT guid, title FROM metadata_items WHERE metadata_items.id = "{}"'.format(ondeck[0]))
        od_meta = cursor.fetchone()
        cursor.execute('''SELECT view_offset FROM metadata_item_settings WHERE metadata_item_settings.guid = "{}"'''.format(od_meta[0]))
        od_offset = cursor.fetchone()
        if not od_offset:
            print("{} at offset {}".format(meta[1],offset[0]))
            return 0
        if od_offset[0]:
            if int(od_offset[0]) >= threshold:
                cursor.execute('SELECT MAX("order") FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}"'.format(playlist_id))
                last = cursor.fetchone();

                # try:
                #     # This helps with Plex's lazy offset handling for audio files
                #     cursor.execute('''UPDATE metadata_item_settings SET view_offset = NULL
                #     WHERE metadata_item_settings.playlist_id = "{}" AND play_queue_generators."order" = "{}"'''.format(playlist_id, last[0]))
                # except:
                #     pass

                cursor.execute('''UPDATE play_queue_generators SET "order" = "order" - 1000
                WHERE play_queue_generators.playlist_id = "{}"'''.format(playlist_id))
                if burn:
                    print("Burning track 1.")
                    cursor.execute('''DELETE FROM play_queue_generators WHERE play_queue_generators."order" = "0.0"'''.format(last[0], playlist_id))
                else:
                    print("Burning and reviving track 1.")

                    # # This helps with Plex's lazy offset handling for audio files
                    # cursor.execute('''UPDATE metadata_item_settings SET view_offset = NULL
                    # WHERE play_queue_generators.playlist_id = "{}" AND metadata_item_settings."order" = "0.0"'''.format(playlist_id))

                    cursor.execute('''UPDATE play_queue_generators SET "order" = {}
                    WHERE play_queue_generators.playlist_id = "{}" AND play_queue_generators."order" = "0.0"'''.format(last[0], playlist_id))
                db.commit()
                return 0

            else:
                print("{} at offset {}, not yet at threshold.".format(od_meta[1],od_offset[0]))
            return od_offset[0]
        else:
            print("Inbetween tracks.")
            return 0
    else:
        print("{} at offset {}".format(meta[1],offset[0]))
        return offset[0]

def add_playlist(db_location):
    db, cursor = db_connect(db_location)
    cursor.execute('SELECT playlist_id FROM play_queues WHERE playlist_id IS NOT NULL')
    playlist_ids = cursor.fetchall();
    pl_name = {}
    for pl_id in playlist_ids:
        pl_id = pl_id[0]
        cursor.execute('SELECT title FROM metadata_items WHERE metadata_items.id = "{}"'.format(pl_id))
        pl_name[pl_id] = cursor.fetchone()[0]
    return pl_name

def update_playlist_id():
    return
    # if pl_name:
    #     print("{} playlists found:".format(len(pl_name)))
    #     for pl_id in pl_name:
    #         print("{}\t-\t{}".format(pl_id, pl_name[pl_id]))
    #     if 'n' in input("Add all playlists? (Y/n): ").lower():
    #         for pl_id in pl_name:
    #             if not 'n' in input("Add "{}" playlist with id {}? (Y/n): ".format(pl_id, pl_name[pl_id])).lower():
    #                 to_add[pl_id] = pl_name[pl_id]
    #     else:
    #         to_add = pl_name
    #     return(to_add)
    # else:
    #     print("No playlists found.")
    #     return




def read_config(cfg):


    config = configparser.ConfigParser()
    if not path.isfile(cfg):
        print("No config file detected. Generating Config File.")
        pathlib(cfg).touch()
    config.read(cfg)

    # Default Settings
    platform_location = ""
    generated = False
    if not config.has_option(None,'db_location'):
        generated = True
        if "win" in platform:
            platform_location = path.expandvars(r'%LOCALAPPDATA%\Plex Media Server\Plug-in Support\Databases\com.plexapp.plugins.library.db')
        else:
            platform_location = input("Can't find database. Please input full path to plex database:")
    config['DEFAULT']['polling'] = config['DEFAULT'].get('polling', '10')
    config['DEFAULT']['db_location'] = config['DEFAULT'].get('db_location', platform_location)
    config['DEFAULT']['backup'] = config['DEFAULT'].get('backup', 'True')
    config['DEFAULT']['backup_level'] = config['DEFAULT'].get('backup_level', '7')

    if not config.sections():
        pl_name = add_playlist(config['DEFAULT']['db_location'])
        if pl_name:
            print("{} playlists found:".format(len(pl_name)))
            for pl_id in pl_name:
                print("\t{}\t-\t{}".format(pl_id, pl_name[pl_id]))
            if 'n' in input("Add all playlists? (Y/n): ").lower():
                for pl_id in pl_name:
                    if not 'n' in input('Add "{}" playlist with id {}? (Y/n): '.format(pl_name[pl_id], pl_id)).lower():
                        generated = True
                        config.add_section(pl_name[pl_id])
                        config[pl_name[pl_id]]['id'] = str(pl_id)
            else:
                generated = True
                for pl_id in pl_name:
                    config.add_section(pl_name[pl_id])
                    config[pl_name[pl_id]]['id'] = str(pl_id)
        else:
            print("No playlists found. Please add a playlist and run the program again.")
            exit()
    for pl in config.sections():
        if not config.has_option(pl,'id'):
            print("All playlists must have an id.")
            print('Removing "{}" Playlist from config file'.format(pl))
            generated = True
            config.remove_section(pl)
            break
        config[pl]['threshold'] = config[pl].get('threshold', '60000')
        config[pl]['burn'] = config[pl].get('burn', 'False')
        config[pl]['timeout_time'] = config[pl].get('timeout_time', '30')
        config[pl]['stall_threshold'] = config[pl].get('stall_threshold', '60')
    if generated:
        if not 'n' in input("Save generated config file? (Y/n): ").lower():
            print('default.cfg backed up to Saving to default.cfg.bak')
            try:
                remove('default.cfg.bak')
                rename('default.cfg', 'default.cfg.bak')
            except:
                pass
            with open('default.cfg', 'w') as configfile:
                print('Saving config to default.cfg')
                config.write(configfile)
    for pl in config.sections():
        config[pl]['timeout'] = 'True'
        config[pl]['stall'] = '0'
        config[pl]['last_off'] = '0'
    return config


def nullify_blobs():
    db, cursor = db_connect(config["DEFAULT"]["db_location"].replace())


def early_retirement():
    db, cursor = db_connect(config["DEFAULT"]["db_location"])
    id_list = [config[pl]["id"] for pl in config.sections()]
    id = ""
    while not id:
        print("\n\nRetire from which playlist?")
        for pl in config.sections():
            print("\t{}\t-\t{}".format(config[pl]['id'], pl))
        try:
            retire_id = int(input("Enter the id number: "))
        except:
            print("Please choose one of the ID Numbers.")
            exit()

        print(retire_id)
        print(id_list)
        if str(retire_id) in id_list:
            id = retire_id
        else:
            print("Please choose one of the ID Numbers.")

    cursor.execute('SELECT metadata_item_id FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}"'.format(id))
    playlist = cursor.fetchall()
    qty = len(playlist)

    proceed = ""
    while not proceed:
        try:
            retire = int(input("How many items to retire? "))
        except:
            print("Integers only!")
            break
        print(playlist[retire][0])
        cursor.execute('SELECT title FROM metadata_items WHERE metadata_items.id = "{}"'.format(playlist[retire][0]))
        name = cursor.fetchone()[0]
        print('Retire {} items. The new first item on playlist will be "{}."'.format(retire, name))
        proceed = input("Is that correct (y/N)? ")
        if not 'y' in  proceed:
            proceed = ""
            break
        else:
            playlist_reset(db, cursor, playlist_id)
            cursor.execute('''UPDATE play_queue_generators SET "order" = "order" + {}
            WHERE play_queue_generators.playlist_id = "{}" AND "order" <= {}'''.format((qty * 1000),id, (retire*1000)))
            playlist_reset(db, cursor, playlist_id)
            db.commit()
            print("Done.")
            db.close()
            exit()




''' Main Body Starts Here '''
config = read_config("default.cfg")
# early_retirement()
db_backup(int(config["DEFAULT"]["backup_level"]), config["DEFAULT"]["db_location"])
try:
    db, cursor = db_connect(config["DEFAULT"]["db_location"])
    while(True):
        for pl in config.sections():
            offset = None
            if config[pl]["timeout"] == 'True':
                if int(config[pl]["stall"]) <= 0:
                    offset = poll(db, cursor, int(config[pl]["id"]), (config[pl]["burn"] == 'True'), int(config[pl]["threshold"]))
                    if offset == int(config[pl]["last_off"]):
                        config[pl]["stall"] = config[pl]["timeout_time"]
                    else:
                        config[pl]["stall"] = "0"
                        config[pl]["timeout"] = "False"
                else:
                    config[pl]["stall"] = str(int(config[pl]["stall"]) - 1)
            else:
                offset = poll(db, cursor, int(config[pl]["id"]), (config[pl]["burn"] == 'True'), int(config[pl]["threshold"]))

                if offset == int(config[pl]["last_off"]):
                    config[pl]["stall"] = str(int(config[pl]["stall"]) + 1)
                    if int(config[pl]["stall"]) >= int(config[pl]["stall_threshold"]):
                        config[pl]["timeout"] = "True"
                        config[pl]["stall"] = config[pl]["timeout_time"]

            config[pl]["last_off"] = str(offset)
        sleep(int(config['DEFAULT']['polling']))


finally:
    db.rollback()
    db.close()
# # playlist reset feature
#
# while True:
#
#     cursor.execute('SELECT metadata_item_id FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}" AND play_queue_generators."order" = 1000.0'.format(playlist_id))
#     watching = cursor.fetchone();
#
#     cursor.execute('SELECT guid, duration FROM metadata_items WHERE metadata_items.id = "{}"'.format(watching[0]))
#     meta = cursor.fetchone();
#
#     cursor.execute('''SELECT view_offset FROM metadata_item_settings WHERE metadata_item_settings.guid = "{}"'''.format(meta[0]))
#     offset = cursor.fetchone();
#
#     if not offset[0]:
#         cursor.execute('SELECT metadata_item_id FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}" AND play_queue_generators."order" = 2000.0'.format(playlist_id))
#         ondeck = cursor.fetchone();
#
#         cursor.execute('SELECT guid, duration FROM metadata_items WHERE metadata_items.id = "{}"'.format(ondeck[0]))
#         meta = cursor.fetchone();
#         cursor.execute('''SELECT view_offset FROM metadata_item_settings WHERE metadata_item_settings.guid = "{}"'''.format(meta[0]))
#         offset = cursor.fetchone();
#
#         if offset[0]:
#             if int(offset[0]) >= threshold:
#                 print("Burning and reviving track 1.")
#                 cursor.execute('SELECT MAX("order") FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}"'.format(playlist_id))
#                 last = cursor.fetchone();
#                 cursor.execute('''UPDATE play_queue_generators SET "order" = "order" - 1000
#                 WHERE play_queue_generators.playlist_id = "{}"'''.format(playlist_id))
#                 if recycle:
#                     cursor.execute('''UPDATE play_queue_generators SET "order" = {}
#                     WHERE play_queue_generators.playlist_id = "{}" AND play_queue_generators."order" = "0.0"'''.format(last[0], playlist_id))
#                 else:
#                     cursor.execute('''DELETE FROM play_queue_generators WHERE play_queue_generators."order" = "0.0"'''.format(last[0], playlist_id))
#                 conn.commit()
#             else:
#                 print("Video 2 begun.")
#         else:
#             print("Playlist halted. Going into sleep mode.") #implement sleep mode.
#     else:
#         print("Still Watching #1")
#     time.sleep(recheck)
#     print("checking again")
# # cursor.execute('SELECT id, metadata_item_id, "order" FROM play_queue_generators WHERE play_queue_generators.playlist_id = "{}"'.format(playlist_id))
# # playlist = cursor.fetchall();
# # print(playlist)
#
#
# conn.close()
