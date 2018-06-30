import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

consumer_key    = 'ABC0d5yEtwEsKaAoXn1vAI25pi'
consumer_secret = 'BwDKaqiR33MkbgpJvDgxu3gLbuARtrlYVXnATtdXyiGMvsMETlD'
access_token    = 'C930962666612580352-kH9V0CZLNfs8hOwCvGGEdfoNg3K3h2X'
access_secret   = 'DID5N3BZCF2ByIPROrecUmwbGoCpv6BYvOrf5EQ2r6AOyH'

class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
		print('\nTWEET START\n')
		print(data.split('\n'))
		self.client_socket.send(data)
		return True
        except BaseException as e:
		print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['vacation'])

if __name__ == "__main__":
    s = socket.socket()     # Create a socket object
    host = "localhost"      # Get local machine name
    port = 5555             # Reserve a port for your service.
    s.bind((host, port))    # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.
    print( "Received request from: " + str( addr ) )

    sendData( c )
