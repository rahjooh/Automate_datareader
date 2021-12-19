import socket
import threading
import os
from subprocess import Popen,PIPE,STDOUT
import sqlite3
import re
import time
import datetime

class ThreadedServer(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target = self.listenToClient,args = (client,address)).start()

    def listenToClient(self, client, address):
        size = 2048
        while True:
            try:
                data = client.recv(size)
                if data:
                    command = data.split()[0]
                    if command == 'getpid':
                        client.send(str(os.getpid()))
                    if command == 'add':
                        id = data.split()[1]
                        data = data.replace('add' + ' ' + id + ' ', '')
                        content = data
                        client.send(addMessage(id, content))

                    else:
                        response = 'command not found'
                        client.send(response)
                else:
                    raise error('Client disconnected')
            except:
                client.close()
                return False

def addMessage(id, content):

    logDir = rootPath + 'logs/'
    logFile = open(logDir + str(id) + '.flog', 'a+')
    logFile.write('%s\t%s\t\n' % (datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S"), content))
    logFile.flush()

    """
    #print(content)
    try:
        con = sqlite3.connect(rootPath + 'log.db', timeout=10)
        cur = con.cursor()
        cur.execute("INSERT INTO tbl_log (ID, content) VALUES ('"+str(id)+"','"+str(content)+"')")
        con.commit()
    except sqlite3.Error as e:
        print("Database error: %s" % e)
    except Exception as e:
        print("Exception in _query: %s" % e)
    finally:
        if con:
            con.close()
    """
    return 'insert'

if __name__ == "__main__":

    p = Popen(['lsof', '-i', ':5556'], stdout=PIPE, stderr=STDOUT)
    output = ''
    for line in p.stdout:
        output = output + str(line)
    if output != '':
        pid = re.search('python  (.*) hduser', output)
        if pid:
            pid = pid.group(1).strip()
            p = Popen(['kill', '-9', str(pid)], stdout=PIPE, stderr=STDOUT)
            time.sleep(3)
        else:
            pid = re.search('java    (.*) hduser', output)
            if pid:
                pid = pid.group(1).strip()
                p = Popen(['kill', '-9', str(pid)], stdout=PIPE, stderr=STDOUT)
                time.sleep(3)
    rootPath = os.path.realpath(__file__)
    temp = rootPath.split('/')
    rootPath = rootPath.replace(temp[-1], "")

    """
    conn = sqlite3.connect(rootPath + 'log.db', timeout=10)
    c = conn.cursor()
    c.execute('''CREATE TABLE if not exists tbl_log 
                     (ID text, content text,ts datetime default current_timestamp)''')
    """
    ip = socket.gethostbyname(socket.gethostname())
    port_num = 5556
    ThreadedServer(ip, port_num).listen()

