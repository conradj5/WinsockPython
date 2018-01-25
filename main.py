import threading
from sys import argv
from queue import Queue
from pysock import Pysock
from socket import timeout
from parsedurl import UrlParser


host_set = set()
host_lock = threading.Lock()
ip_set = set()
ip_lock = threading.Lock()
q = Queue()


def unique_host(host):
    with host_lock:
        if host not in host_set:
            host_set.add(host)
            return True
        return False


def unique_ip(ip):
    with ip_lock:
        if ip not in ip_set:
            ip_set.add(ip)
            return True
        return False


def run(thread_num):
    while True:
        url = q.get()
        print("Thread #" + str(thread_num) + " received URL " + url)
        # parse url #
        try:
            parsed_url = UrlParser(url)
        except Exception as e:
            print("FAILED TO PARSE URL: " + url)
            q.task_done()
            pass

        # check if host is unique #
        if not unique_host(parsed_url.host):
            q.task_done()
            pass

        # create socket #
        ws = Pysock()

        ws.connect(parsed_url.ip, parsed_url.port)
        print('Thread #{} - Successfully connected to {} <{}> on port {}'.format(thread_num, parsed_url.host, parsed_url.ip, parsed_url.port))
        # Construct the GET request #
        get_req = 'GET ' + parsed_url.path + parsed_url.query + ' HTTP/1.0\nUser-agent: udaytoncrawler/1.0\n\nHost: ' + parsed_url.host + '\nConnection:close\n\n'
        ws.send(get_req.encode())
        print('Thread #{} - Successfully sent request'.format(str(thread_num)))
        try:
            data = ws.receive()
        except timeout:
            q.task_done()
            print("Thread #" + str(thread_num) + " timeout")
            pass
        ws.sock.close()
        q.task_done()
        print("Thread #" + str(thread_num) + " complete")



for x in range(int(argv[1])):
    t = threading.Thread(target=run, args=[x])
    t.daemon = True
    t.start()

with open(argv[2]) as file:
    for line in file:
        q.put(line)

# block until all items in queue call task_done()
q.join()
print("done")
