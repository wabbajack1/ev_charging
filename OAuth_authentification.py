#!/usr/bin/env python3

import time
import urllib.parse
import hashlib
from base64 import b64encode  #Needed for create_signature function
import hmac     #Needed for create_signature function
import hashlib  #Needed for create_signature functionx
import binascii #Needed for create_signature function
import requests


grant_type = 'client_credentials'
oauth_consumer_key = 'eLrmG81BaxXmi-2R1KQebQ' #From credentials.properties file
oauth_nonce = str(int(time.time()*1000))
oauth_signature_method = 'HMAC-SHA256'
oauth_timestamp = str(int(time.time()))
oauth_version = '1.0'

url = 'https://account.api.here.com/oauth2/token'

def create_parameter_string(grant_type, oauth_consumer_key,oauth_nonce,oauth_signature_method,oauth_timestamp,oauth_version) -> str:
    parameter_string = ''
    parameter_string = parameter_string + 'grant_type=' + grant_type
    parameter_string = parameter_string + '&oauth_consumer_key=' + oauth_consumer_key
    parameter_string = parameter_string + '&oauth_nonce=' + oauth_nonce
    parameter_string = parameter_string + '&oauth_signature_method=' + oauth_signature_method
    parameter_string = parameter_string + '&oauth_timestamp=' + oauth_timestamp
    parameter_string = parameter_string + '&oauth_version=' + oauth_version
    return parameter_string

parameter_string = create_parameter_string(grant_type, oauth_consumer_key,oauth_nonce,oauth_signature_method,oauth_timestamp,oauth_version)

encoded_parameter_string = urllib.parse.quote(parameter_string, safe="")



encoded_base_string = 'POST' + '&' + urllib.parse.quote(url, safe='')
encoded_base_string = encoded_base_string + '&' + encoded_parameter_string


access_key_secret = 'YZ1xY9YGzGxxKcMISThDXzLOysJ82cKOFhbb3lomCJPGLCXzi6EUn2_9-IiNd50CCnQOgIsLaBhxWL0Xtjzrug' #From credentials.properties file
signing_key = access_key_secret + '&'


def create_signature(secret_key, signature_base_string):
    encoded_string = signature_base_string.encode()
    encoded_key = secret_key.encode()
    temp = hmac.new(encoded_key, encoded_string, hashlib.sha256).hexdigest()
    byte_array = b64encode(binascii.unhexlify(temp))
    return byte_array.decode()

oauth_signature = create_signature(signing_key, encoded_base_string)

encoded_oauth_signature = urllib.parse.quote(oauth_signature, safe='')


body = {'grant_type' : '{}'.format(grant_type)}

headers = {
            'Content-Type' : 'application/x-www-form-urlencoded',
            'Authorization' : 'OAuth oauth_consumer_key="{0}",oauth_nonce="{1}",oauth_signature="{2}",oauth_signature_method="HMAC-SHA256",oauth_timestamp="{3}",oauth_version="1.0"'.format(oauth_consumer_key,oauth_nonce,encoded_oauth_signature,oauth_timestamp)
          }
    
response = requests.post(url, data=body, headers=headers)

import json

x = json.loads(response.text)

#print(x['access_token'])
print(response.text)
