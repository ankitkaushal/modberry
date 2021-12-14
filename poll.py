from pymodbus.client.sync import ModbusSerialClient
import json
import os
from time import sleep
import time
import logging
import sqlite3

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

initializeDB(db_path,config)


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
                    titles = ",".join(titles)
                    values = ",".join(values)       
                    query = f"insert into device{device['address']}({titles}) values({values})"                    
                    conn.execute(query)
            else:
                logging.error(res)

    else:
        logging.error('Cannot connect to the Modbus Server/Slave')
    
    sleep(config['polling_interval'])

