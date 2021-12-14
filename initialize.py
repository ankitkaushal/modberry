import sqlite3
import json
import os

def initializeDB(dbPath, config):    
    with sqlite3.connect("database.db") as conn:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")        
        if len(list(tables)) == 0:
            for device in config['devices']:
                query = f"create table device{device['address']}(timestamp number,"
                device_config_path = os.path.join(os.getcwd(),"types",f"{device['type']}.json")
                with open(device_config_path) as device_file:
                    device_config = json.loads(device_file.read())
                
                for item in device_config['mapping'].items():
                    query +=f"{item[1]} number,"
                query = query[:-1]
                query +=")"
                conn.execute(query)
                    

