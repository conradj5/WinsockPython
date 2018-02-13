import socket


class Pysock:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(10)  # 10 second timeout

    def connect(self, ip, port):
        self.sock.connect((ip, port))

    def send(self, req):
        self.sock.send(req)

    def receive(self):
        resp = b""
        buf_size = 1024 * 1024
        buf = self.sock.recv(buf_size)

        while len(buf) == buf_size:
            resp += buf
            if len(resp) > 2097152:
                raise socket.timeout
            buf = self.sock.recv(buf_size)

        resp += buf

        for i in ['utf-8', 'ISO-8859-1']:
            try:
                return resp.decode(i)
            except UnicodeDecodeError:
                pass

        return ""
