
import sys
import socket
import SocketServer
import time
#from socketserver import BaseRequestHandler, TCPServer

class EchoHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        print('Got connection from', self.client_address)
        #while True:
        
        f=file('/home/cloudera/share/Lab2-WC-Input.txt','r')
        i=0
        while True: 
        		i=i+1
        		line=f.readline()
        		self.request.send(line)
						
        		if len(line)==0: # Zero length indicates EOF 
						   break
								
        		if (i%20==0):
								print('sleep')
								time.sleep(1)
						
						
								'''
						with open("/home/cloudera/share/Lab2-WC-Input.txt",'r') as f:
    				for line in f:
        				self.request.send(line)
        		break
        				
            msg = self.request.recv(1024)
            if not msg:
                break
            self.request.send(msg)
            '''

if __name__ == '__main__':
	
	if len(sys.argv) != 2:
	    print >> sys.stderr, "Usage:  <port>"
	    exit(-1)
	
	print('listen '+sys.argv[1])
	serv = SocketServer.TCPServer(("localhost", int(sys.argv[1])), EchoHandler)
	serv.serve_forever()