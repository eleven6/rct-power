#!/usr/bin/env python3
import socket
import sys
import datetime
import time
import struct

# define hostname and port

host = '172.27.1.53'  	# The server's hostname or IP address
port = 8899            	# The port used by the server
server_address = (host, port) # combination

# define variables needed
msg = ""                # The message we receive from the socket

# function to get the current timestamp for logging 
def get_time():
    now = datetime.datetime.now() # 
    return now.strftime('%Y-%m-%dT%H:%M:%S')


# function to convert the results to human readable by splitting it into different messages (=frames) according to RCT spec.

#Method which takes a raw byte input stream (converted to a string) and splits it into different messages (=frames) according to RCT spec.
#	 * 
#	 * Start of stream character is 2B, this will be removed. If string doesn't start with 2B then null will be returned. The input string
#	 * will be split at every 2B unless preceded by a 2D (=escape character).
#	 * 
#	 * @param inputString, this is the raw input string received from the TCP/IP stream. The byte stream needs to be converted to string first.
#	 * @return an array of Strings, each string potentially represents a set of data. Further filtering is still necessary.
#    check if the stream includes the start_byte 002B prior to processing, cancel if false
#    We have to split the stream into the different messages/frames
#    a frame has the following structure according to RCT spec
#    1 start byte (2B) | start string (002B)  
#    1 command byte, 05 for short read or 06 for long read
#    1 byte for the length (2 bytes for long read) value: 4+n, 4 bytes for the command, n bytes of data
#    4 bytes for the id
#    n bytes of data
#    2 bytes CRC = 4 characters
#
# Be aware that the string contains characters and two characters are one byte. 
# Hence there is a factor of 2 between string size and byte size.


def convert_results(msg):
    # print message once
    #print(get_time() + ' <<DEBUG>> Message=' + msg)

    # check for start_byte and begine conversion if TRUE
    if msg[:4] == "002B":
       # remove start_byte
       msg = msg[4:]
  
       # check command type, we expect a response type -> either 05 or 06
       # RCT Commands: 01=Read, 02=Write, 03=Long Write, 04=reserved, 05=Response, 06=Long Response
       
       # let's capture the command
       cmd = msg[:2]
       if cmd == '05' or cmd == '06':
          #print('Command: ' + cmd)
          
          # remove command
          msg = msg[2:]
          
          # check length & remove, attention: if long response, we have 4 character (2 bytes), otherwise only 1
          if cmd == '06':
             length = msg[:4]
             msg = msg[4:]
          else:
             length = msg[:2]
             msg = msg[2:]
          
          # print('Length: ' + length + " = " + str(int(length, 16)))
                    
          # check rctid & remove
          rctid = msg[:8]
          msg = msg[8:]
          print ('RCT ID found: ' + rctid);
          # print('Remaining Message: ' + msg)
                    
          # check CRC & remove
          crc = msg[-4:]
          msg = msg[:-4]
          # print('CRC: ' + crc)
          
          # process the results
          # for command type 5 we get a direct value nothing else
          if cmd == '05':
              size = len(msg)
              # print(get_time() + ' <<DEBUG>> message length = ' + str(size))
              if size == 8:
                value = struct.unpack('!f', bytes.fromhex(msg[:len(msg)]))[0]
                print(get_time() + ' value=' + str(value))
              #else:
                # print(get_time() + ' <<DEBUG>> message is too short (' + str(size) + '), something is wrong. Message=' + msg)
              
          else:
              # extract request timestamp
              request_time = int(msg[:8], 16)
              print('Request_Time: ' + datetime.datetime.utcfromtimestamp(request_time).strftime('%Y-%m-%d %H:%M:%S'))
              
              # remove request timestamp from message
              msg = msg[8:]
              
              # prepare varaibles value_timestamp (Linux Timestamp) & value (float32)
              value_timestamp = 0
              value = 0.0
              data = []
              i = 0
              
              # process all remaining messages
              while len(msg) > 0:
                 # check that we have enough messages left value_timestsamp = 8 characters + value = 8 characters
                 if len(msg) >= 16:
                    value_timestamp = int(msg[:8], 16)
                    msg = msg[8:]
                    value = struct.unpack('!f', bytes.fromhex(msg[:8]))[0]
                    msg = msg[8:]
                    element = [value_timestamp, value]
                    print('Result: Time=' + datetime.datetime.utcfromtimestamp(value_timestamp).strftime('%Y-%m-%d %H:%M:%S') + ' | Value=' + str(value))
                    data.append(element)
                 if len(msg) < 16:
                     msg = ""
        
       else:
         print(get_time() + ' <<DEBUG>> response command (05-short or 06-long) expected but not found, ignoring message');
     
    else:
      print(get_time() +  ' <<DEBUG>> start byte not found, ignoring message')  
     


# now just listen

try:

    # Create the TCP/IP socket & connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    print (get_time() + ' <<DEBUG>> listenting on socket ' + host + ':' + str(port))
    
    # Look for the response
    amount_received = 0
    amount_expected = 16 # minimum 16 character
    
    
    while amount_received < amount_expected and amount_received != -1:
        now = datetime.datetime.now() # refresh each time
        data = sock.recv(2048)
        amount_received += len(data)
        print (get_time() + ' <<DEBUG>> Message received with length: ' + str(amount_received))
        s = data.hex()
        print('Message: ' + s.upper())
        convert_results(s.upper())
        
        # reset amount received and continue to listen
        amount_received = 0
        #print('sleep...')
        #time.sleep(2) # sleep for 5 seconds
        # nach 5 Minuten ohne Aktivität stoppt der Inverter die Verbindung d.h. man müsste für ein Dauermonitoring den Socket alle 5min schliessen und neu öffnen >> die Nachricht wenn die Verbindung geschlossen wird hat die Länge -1
        

finally:
    print ('Closing Socket')
    sock.close()
