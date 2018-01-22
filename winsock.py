import socket
import urllib


class Winsock:
    sock = None

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self, ip, port):
        self.sock.connect((ip, port))

    def send(self, req):
        self.sock.send(req)

    def receive(self):
        resp = b""
        buf = 1024

        bytes = self.sock.recv(buf)
        while len(bytes) > 0:
            resp += bytes
            bytes = self.sock.recv(buf)

        for i in ['utf-8', 'ISO-8859-1']:
            try:
                return resp.decode(i)
            except UnicodeDecodeError:
                pass

        return ""
