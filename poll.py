from pymodbus.client.sync import ModbusSerialClient
import json
import os
from time import sleep
import time
import logging
import sqlite3
import azure.cosmos.cosmos_client as cosmos_client

from initialize import initializeDB



logging.basicConfig(level=logging.INFO, 
                    filename="logs/error.log", # log to this file
                    format='%(asctime)s %(message)s') # include timestamp

config_path = os.path.join(os.getcwd(),"config.json")

db_path = "database.db"

with open(config_path) as file:
    config = json.loads(file.read())


client = ModbusSerialClient(
    method='rtu',
    port=config['port'],
    baudrate=config['baudrate'],
    timeout=config['timeout'],
    parity=config['parity'],
    stopbits=config['stopbits'],
    bytesize=config['bytesize']
)

if not config['debug']:
    initializeDB(db_path,config)

    if config['remote']["enabled"]:
        cosmosClient = cosmos_client.CosmosClient(config['remote']['host'],{'masterKey':config['remote']['key']},user_agent = "pi",user_agent_overwrite=True)
        db = cosmosClient.get_database_client(config['remote']['db'])
        container = db.get_container_client(config['remote']['container'])

def createEntry(dbEntry):
    container.create_item(body=dbEntry)


def logError(message):
    if config['debug']:
        print(message)
    else:
        logging.error(message)

while True:
    if client.connect():  # Trying for connect to Modbus Server/Slave
        '''Reading from a holding register with the below content.'''
        for device in config['devices']:
            device_config_path = os.path.join(os.getcwd(),"types",f"{device['type']}.json")
            with open(device_config_path) as device_file:
                device_config = json.loads(device_file.read())
            
            res = client.read_input_registers(address=device_config['start']-1, count=device_config['count'], unit=device['address'])       

            if not res.isError():
                with sqlite3.connect("database.db") as conn:
                    titles=['timestamp']
                    values =[str(time.time())]                    
                    for item in device_config["mapping"].items():
                        values.append( str(res.registers[int(item[0]) - device_config['start'] ]))
                        titles.append(item[1])                 
                    titlesStr = ",".join(titles)
                    valuesStr = ",".join(values)       
                    query = f"insert into device{device['address']}({titlesStr}) values({valuesStr})"                    
                    if config['debug']:
                        print(query)
                    else:
                        conn.execute(query)
                        if(config['remote']['enabled']):
                            dbItem = dict(zip(titles,values))
                            dbItem['deviceId'] = device['address']
                            dbItem['siteId'] = config['site']
                            dbItem['id'] = dbItem['siteId'] + str(dbItem['deviceId']) + str(dbItem['timestamp'])                            
                            createEntry(dbItem)
                        

            else:
                logError(res)
    else:
        logError('Cannot connect to the Modbus Server/Slave')    

    sleep(config['polling_interval'])

