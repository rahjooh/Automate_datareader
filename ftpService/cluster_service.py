import socket
import threading
import os
from subprocess import Popen,PIPE,STDOUT
import re
import sys
import time

def sendMessage(id, text):

    server_address = ('10.100.136.60', 5556)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)

    except socket.error, e:
        if 'Connection refused' in e:
            print('Connection refused')
            return 'Connection refused'
    try:
        message = 'add' + ' ' + id + ' ' + text
        sock.sendall(message)
        try:
            response = sock.recv(4096)
        except:
            print('Oh noes! %s' % sys.exc_info()[0])
            return False
    finally:
        sock.close()
    return True

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
        size = 1024
        while True:
            try:
                data = client.recv(size)
                if data:
                    command = data.split()[0]
                    if command == 'getpid':
                        client.send(str(os.getpid()))
                    if command == 'unzip':
                        filename = data.split()[1]
                        password = data.split()[2]
                        randstr = data.split()[3]
                        client.send(unzipfile(filename, password, randstr))
                    elif command == 'stopthriftserver':
                        client.send(stopthriftserver())
                    elif command == 'startthriftserver':
                        client.send(startthriftserver())
                    else:
                        response = 'command not found'
                        client.send(response)
                else:
                    raise error('Client disconnected')
            except:
                client.close()
                return False

def startthriftserver():

    command = '/home/' + os.getlogin() + '/spark/sbin/start-thriftserver.sh'
    p = Popen([command], stdout=PIPE, stderr=STDOUT)
    output = ''
    for line in p.stdout:
        output = output + str(line)
    sendMessage('thriftserver', output)
    return output

def stopthriftserver():

    command = '/home/' + os.getlogin() + '/spark/sbin/stop-thriftserver.sh'
    p = Popen([command], stdout=PIPE, stderr=STDOUT)
    output = ''
    for line in p.stdout:
        output = output + str(line)
    sendMessage('thriftserver', output)
    return output

def unzipfile(filename, password, randstr):
    sendMessage(randstr, 'get connection to unzip ' + filename + ' with pass : ' + password)
    local_file = os.path.join(rootPath, filename)
    if os.path.isfile(local_file):
        if filename.lower().endswith('zip'):
            #   unzip command by bash
            p = Popen(['unzip', '-o', '-P', password, local_file], stdout=PIPE, stderr=STDOUT)
            print("after send bash command")
            output = ''
            for line in p.stdout:
                output = output + str(line)
            if output.find('incorrect password') == True:
                os.remove(local_file)
                sendMessage(randstr, 'password wrong.')
                return 'password wrong.'
            else:
                #   rename file to _tmp
                if 'inflating: ' in output:
                    name = re.search('inflating:(.*) ', output)
                    name = name.group(1)
                    name = name.replace(' ','')
                    name = os.path.join(rootPath, name)
                    sendMessage(randstr, 'nameing is|' + str(name) + '|')

                    unzipfile = filename.replace('.zip', '') + '_tmp'
                    unzipfile = os.path.join(rootPath, unzipfile)
                    os.rename(name, unzipfile)
                    sendMessage(randstr, 'name is ' + name)
                    sendMessage(randstr, 'newname is ' + unzipfile)
                    sendMessage(randstr, 'rename finish')

                    #   hadoop put command
                    p = Popen(['/home/hduser/hadoop/bin/hadoop', 'fs', '-put', unzipfile, '/user/hduser/ftp'], stdout=PIPE, stderr=STDOUT)
                    output = ''
                    for line in p.stdout:
                        output = output + str(line)

                    # Remove files
                    os.remove(local_file)
                    os.remove(unzipfile)
                    sendMessage(randstr, 'Done!')
                    return 'Done!'
                return
        else:
            return 'file does not zip file.'
    else:
        return 'file not found.'

if __name__ == "__main__":

    p = Popen(['lsof', '-i', ':5555'], stdout=PIPE, stderr=STDOUT)
    output = ''
    for line in p.stdout:
        output = output + str(line)
    if output != '':
        pid = re.search('python  (.*) hduser', output)
        if pid:
            pid = pid.group(1).strip()
            print(pid)
            p = Popen(['kill', '-9', str(pid)], stdout=PIPE, stderr=STDOUT)
            time.sleep(3)
        else:
            pid = re.search('java    (.*) hduser', output)
            if pid:
                pid = pid.group(1).strip()
                print(pid)
                p = Popen(['kill', '-9', str(pid)], stdout=PIPE, stderr=STDOUT)
                time.sleep(3)
    #while True:
    #    port_num = input("Port? ")
    #    try:
    #        port_num = int(port_num)
    #        break
    #    except ValueError:
    #        pass
    rootPath = os.path.realpath(__file__)
    temp = rootPath.split('/')
    rootPath = rootPath.replace(temp[-1], "")
    ip = socket.gethostbyname(socket.gethostname())
    port_num = 5555
    ThreadedServer(ip, port_num).listen()

