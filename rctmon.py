#!/usr/bin/env python3
import socket
import sys
import datetime
import time
import struct
import math as m
import paho.mqtt.client as mqtt

# Define Variables
host = '172.27.1.53'    # The server's hostname or IP address
port = 8899             # The port used by the server
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # The socket
success_counter = 0 # track if all messages where received
mqtt_host = '172.27.1.32' # address of MQTT broker
mqtt_port = 1883 # port of MQTT broker
mqtt_topic = 'monitoring/pv' # the topic that will be used
mqttc = mqtt.Client() # the mqtt client connection

id_list = [] # id list, select the short only since we want the current numbers only

# current values
id_list.append([ "B55BA2CE", "04", "" ]) # Input A in Volt
id_list.append([ "DB11855B", "04", "" ]) # Input A in Watt
id_list.append([ "B0041187", "04", "" ]) # Input B in Volt
id_list.append([ "0CB5D21B", "04", "" ]) # Input B in Watt
id_list.append([ "DB2D69AE", "04", "" ]) # Inverter Current Power
id_list.append([ "A7FA5C5D", "04", "" ]) # Battery Voltage
id_list.append([ "8B9FF008", "04", "" ]) # Battery Upper SOC Limit in %
id_list.append([ "959930BF", "04", "" ]) # Battery State of Charge in %
id_list.append([ "400F015B", "04", "" ]) # Battery Power (positive: discharge, negative: charge) in Watt
id_list.append([ "4BC0F974", "04", "" ]) # Battery gross capacity in Watt
id_list.append([ "902AFAFB", "04", "" ]) # Battery Temperature in °C
id_list.append([ "1AC87AA0", "04", "" ]) # Current Household Power Consumption in Watt
id_list.append([ "91617C58", "04", "" ]) # Current Grid Feed (positive: we feed, negative: we surge)

# daily values
id_list.append([ "BD55905F", "04", "" ]) # Today's energy Wh
id_list.append([ "2AE703F2", "04", "" ]) # Today's energy returns Input A in Wh
id_list.append([ "FBF3CE97", "04", "" ]) # Today's energy returns Input B in Wh
id_list.append([ "3C87C4F5", "04", "" ]) # Today's grid feed in -Wh
id_list.append([ "867DEF7D", "04", "" ]) # Today's grid load in Wh
id_list.append([ "2F3C1D7D", "04", "" ]) # Today's energy consumption household in Wh

# monthly values
id_list.append([ "10970E9D", "04", "" ]) # This month's energy [Wh]
id_list.append([ "81AE960B", "04", "" ]) # This month's energy returns Input A in Wh
id_list.append([ "7AB9B045", "04", "" ]) # This month's energy returns Input B in Wh
id_list.append([ "65B624AB", "04", "" ]) # This month's grid feed in -Wh
id_list.append([ "126ABC86", "04", "" ]) # This month's grid load in Wh
id_list.append([ "F0BE6429", "04", "" ]) # This month's energy consumption household in Wh

# yearly values
#id_list.append([ "C0CC81B6", "04", "" ]) # This years's energy [Wh]
#id_list.append([ "AF64D0FE", "04", "" ]) # This year's energy returns Input A in Wh
#id_list.append([ "BD55D796", "04", "" ]) # This year's energy returns Input B in Wh
#id_list.append([ "26EFFC2F", "04", "" ]) # This year's grid feed in -Wh
#id_list.append([ "DE17F021", "04", "" ]) # This year's grid load in Wh
#id_list.append([ "C7D3B479", "04", "" ]) # This year's energy consumption household in Wh

# total values
#id_list.append([ "B1EF67CE", "04", "" ]) # Total energy [Wh]
#id_list.append([ "FC724A9E", "04", "" ]) # Total energy returns Input A in Wh
#id_list.append([ "68EEFD3D", "04", "" ]) # This year's energy returns Input B in Wh
#id_list.append([ "44D4C533", "04", "" ]) # This year's grid feed in -Wh
#id_list.append([ "62FBE7DC", "04", "" ]) # This year's grid load in Wh
#id_list.append([ "EFF4B537", "04", "" ]) # This year's energy consumption household in Wh



# function to get the current timestamp for logging 
def get_time():
    now = datetime.datetime.now() # 
    return now.strftime('%Y-%m-%dT%H:%M:%S')

# function to print all results
def print_list(list):
    for data in list:
        print('RCTID: ' + data[0] + ' has value: ' + data[2])

# will be executed when mqttc.connect receives response
def on_connect(client, userdata, flags, rc):
    global mqtt_topic # make topic available
    global mqttc 
    
    if rc==0:
       #print("succesfully connected...")
       mqttc.subscribe(mqtt_topic)
    else:
       print("connection could not be established...")
       sys.exit(1)

# will be executed when mqttc.disconnect receives repsonse
def on_disconnect(client, userdata, rc):
  #print("successfully disconnected...")
  pass

# function to send data via MQTT to OpenHAB
def mqtt_send_data(id_list):
    global mqtt_topic # make topic available
    global mqtt_host # mqtt hostname
    global mqtt_port # mqtt port
    global mttqc # make the mqttc client available
    
    # connect for broadcasting
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    
    try:
      mqttc.connect(mqtt_host, port=1883, keepalive=60, bind_address="")
      mqttc.loop_start()
    except:
       print("could not connect to MQTT-Broker at host:" + mqtt_host)
       sys,exit(1)
       
    for data in id_list:
      subtopic = mqtt_topic + '/' + data[0]
      if data[2] != "null":
         mqttc.publish(subtopic, payload=data[2], qos=0, retain=False)   
      else:
         mqttc.publish(subtopic, payload=0, qos=0, retain=False)   
      print(get_time() + ' data sent to MQTT ' + subtopic) 
    # send timestamp of update
    subtopic = mqtt_topic + '/lastupdate'
    mqttc.publish(subtopic,  payload=get_time(), qos=0, retain=False)  
    
    # disconnect
    mqttc.loop_stop()
    mqttc.disconnect()

# function to calculate CRC
def calc_crc(command):
    print ("Calculating Command CRC....")
    command_length = m.ceil(len(command)/2) # to determine if the command is even or odd
    crc = 0xFFFF # set a default CRC
 
    for x in range (0, command_length):
      b = int(command[x*2:x*2+2],16)
      for i in range (0, 8):
        bit = int((( b >> (7-i) & 1) == 1))
        if bit == "0":
           bit = ''
        c15 = int(((( crc >> 15) & 1) == 1))
        crc <<= 1
        if c15 ^ bit:
           crc ^= 0x1021
      crc &= 0XFFFF
    crc = hex(crc)[2:].upper()
    if len(crc) == 3:
       crc = '0' + crc
    return(crc)
 
# function to request data from inverter
def request_command(id, length):
    print("building the request command...")
    start_byte = "2B" # the start byte as per 
    command = "01" # 01: read 02: write 03: long-write
    request_command = command + length + id # combine request string
    crc = calc_crc(request_command) # calculate CRC 
    request_command = start_byte + request_command + crc; # combine final request string
    request_command = request_command.upper()
    request_command = request_command.replace("2D", "2D2D") #escape 2D with a proceeding 2D for escaping: 2D >> 2D2D
    request_command = request_command.replace("2B", "2D2B") #escape 2B with a proceeding 2D for escaping: 2B >> 2D2B
    return request_command # return the final requst command

# function to clean the response
def clean_response(id, response):
    response = response.replace("2D2D", "2D") #un-escape 2D with a proceeding 2D for escaping: 2D2D >> 2D
    response = response.replace("2D2B", "2B") #un-escape 2B with a proceeding 2D for escaping: 2D2B >> 2B

    if response[:4] == "002B": # remove start_byte
       response = response[4:]
       response = response[:-4] # remove CRC: last 4 characters / 2 bytes
       response = response[2:] # remove command
       response = response[2:] # remove length
       rctid = response[:8] # extract rct_request_id    
       if rctid == id:
          response = response[8:] # remove rct_request_id
       else:
          response = "" # empty response b/c something went wrong
       if len(response) != 8:
          response = "" # empty response b/c something went weong
    else:
       response = "" # empty response b/c something went wrong
    
    return response

# process the RCT data update request
def request_update(data):
    # build the request_command
    request = request_command(data[0], data[1]) # ID + Length
    response_length = 0 # needs to change from 0 to 28
    response_valid = 0 # needs to change from 0=false to 1=true
    value = ""
    counter = 0
    
    print ("entering request query,...")

    # expected response_length = 28, sometimes we get dirty data, directly retry
    while (response_length != 28) & (response_valid != 1):
        # send request_command to inverter
        sock.sendall(bytes.fromhex(request)) 
        print ("send request " + request)
        
        # sleep
        time.sleep(0.25) 
        
        # receive response
        response = sock.recv(2048)
        response = response.hex().upper()

        print ("Received response " + response)
        
        # check response length
        response_length = len(response)
   
        # clean the response
        response = clean_response(data[0], response)
        
        # incrase counter
        counter += 1 
        print ("Data retrieval for " + data[0] + " attempt: " + str(counter))

        # check if all is good
        if response != "":
           response_valid = 1
   
           # convert hex2float
           value = str(round(struct.unpack('!f', bytes.fromhex(response[:len(response)]))[0],4))

           print (data[0] +  " hase value " + value)
         
        # error handling, stop after 10 attempts
        if (counter == 10):
           value = "0"
           response_valid = 1
           print ("something when wrong retrieving data for " + data[0])
    
    return value

# connect
print(get_time() + ' Update started')
sock.connect((host, port))

# update data
for data in id_list:
   # pass the object to request_update
   print('requesting update for ' + data[0])
   data[2] = request_update(data)
   time.sleep(1)
   
# send list via mqtt
mqtt_send_data(id_list)

# print results
print_list(id_list)

# disconnect
sock.close()
print(get_time() +' Update completed')
