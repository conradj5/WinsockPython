from winsock import Winsock
from parsedurl import UrlParser

# parse url #
parsed_url = UrlParser("http://www.google.com/")
# create socket #
ws = Winsock()
ws.connect(parsed_url.ip, parsed_url.port)
print('Successfully connected to {} <{}> on port {}'.format(parsed_url.host, parsed_url.ip, parsed_url.port))
# Construct the GET request #
GET = 'GET ' + parsed_url.path + parsed_url.query + ' HTTP/1.0\nHost: ' + parsed_url.host + '\n\n'
ws.send(GET.encode())
data = ws.receive()
ws.sock.close()

print(data)
