import threading
from sys import argv
from sys import exit
from time import time
from queue import Queue
from pysock import Pysock
from socket import timeout
from parsedurl import UrlParser
from socket import gethostbyname, gaierror

host_set = set()
host_lock = threading.Lock()
ip_set = set()
ip_lock = threading.Lock()
queue = Queue()

get_time = lambda: int(round(time() * 1000))

head_req = 'HEAD /robots.txt HTTP/1.0\nUser-agent: udaytoncrawler/1.0\nHost: {}\nConnection:close\n\n'
get_req = 'GET {}{} HTTP/1.0\nUser-agent: udaytoncrawler/1.0\nHost: {}\nConnection:close\n\n'


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


def fail(msg):
    msg += "failed\n"
    print(msg)
    queue.task_done()


def run(thread_num):
    while True:
        url = queue.get()
        message = "Thread #" + str(thread_num) + " - URL:" + url

        # parse url #
        message += "\tParsing URL... "
        try:
            parsed_url = UrlParser(url)
        except:
            fail(message)
            continue
        message += "host {}, port {}\n".format(parsed_url.host, parsed_url.port)

        # check if host is unique #
        message += "\tChecking host uniqueness... "
        if not unique_host(parsed_url.host):
            fail(message)
            continue
        message += "passed\n"

        # DNS for IP#
        message += "\tDoing DNS... "
        start = get_time()
        try:
            parsed_url.ip = gethostbyname(parsed_url.host)
            if parsed_url:
                message += "done in {}ms, found {}\n".format(get_time() - start, parsed_url.ip)
            else:
                fail(message)
                continue
        except (Exception, AttributeError):
            fail(message)
            continue

        # check is IP is unique #
        message += "\tChecking IP uniqueness... "
        if not unique_ip(parsed_url.ip):
            fail(message)
            continue
        message += "passed\n"

        # create socket and connect to server #
        ws = Pysock()
        message += "\tConnecting on robots... "
        start = get_time()
        try:
            ws.connect(parsed_url.ip, parsed_url.port)
        except timeout:
            fail(message)
            continue
        message += " done in {}ms\n".format(get_time()-start)

        # Send HEAD request #
        ws.send(head_req.format(parsed_url.host).encode())

        # load response #
        message += "\tLoading... "
        start = get_time()
        try:
            head_resp = ws.receive()
        except timeout:
            fail(message)
            continue
        message += "done in {}ms with {} bytes\n".format(get_time()-start, len(head_resp))

        # verify header if robot allowed #
        code = head_resp[9:12]
        message += "\tVerifying header... status code {}\n".format(code)
        if head_resp[9] is not '4':
            print(message)
            queue.task_done()
            continue

        # ask Dr. Yao if we must open a new socket #
        ws.sock.close()
        # create new socket and connect #
        ws = Pysock()
        message += "\tConnecting on page... "
        start = get_time()
        try:
            ws.connect(parsed_url.ip, parsed_url.port)
        except timeout:
            fail(message)
            continue
        message += "done in {}ms\n".format(get_time()-start)

        # Send the GET request #
        ws.send(get_req.format(parsed_url.path, parsed_url.query, parsed_url.host).encode())

        # load data #
        message += "\tLoading... "
        start = get_time()
        try:
            get_resp = ws.receive()
        except timeout:
            fail(message)
            continue
        message += "done in {}ms with {} bytes\n".format(get_time() - start, len(get_resp))

        # verify header #
        code = get_resp[9:12]
        message += "\tVerifying header... status code {}\n".format(code)
        if get_resp[9] is not '2':
            print(message)
            queue.task_done()
            continue

        # parse for urls #

        # task done #
        ws.sock.close()
        queue.task_done()
        print(message)


for x in range(1, int(argv[1])+1):
    t = threading.Thread(target=run, args=[x, ])
    t.daemon = True
    t.start()

try:
    with open(argv[2]) as file:
        print("Opened " + argv[2])
        for line in file:
            queue.put(line)
except IOError:
    print('No such file: ' + argv[2] + '\n')
    exit()

# block until all items in queue call task_done() #
queue.join()
print("done")
