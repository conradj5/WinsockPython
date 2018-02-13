from time import time
from re import findall
from queue import Queue
from pysock import Pysock
from sys import exit, argv
from parsedurl import UrlParser
from collections import Counter
from threading import Thread, Lock
from socket import gethostbyname, error as SocketError

host_set = set()
host_lock = Lock()
ip_set = set()
ip_lock = Lock()
DATA = Counter()
DATA_LOCK = Lock()
queue = Queue()

get_time = lambda: int(round(time() * 1000))

head_req = 'HEAD /robots.txt HTTP/1.0\r\nHost: {}\r\nUser-agent: udaytoncrawler/1.0\r\n\r\n'
get_req = 'GET {}{} HTTP/1.0\r\nHost: {}\r\nUser-agent: udaytoncrawler/1.0\r\n\r\n'


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
    # print(msg)
    # queue.task_done()


def run_task(url):
    data = Counter()
    message = "URL: " + url

    # parse url #
    message += "\tParsing URL... "
    try:
        parsed_url = UrlParser(url)
    except:
        fail(message)
        return data
    message += "host {}, port {}\n".format(parsed_url.host, parsed_url.port)

    # check if host is unique #
    message += "\tChecking host uniqueness... "
    if not unique_host(hash(parsed_url.host)):
        fail(message)
        return data
    message += "passed\n"
    data['hosts'] = 1

    # DNS for IP#
    message += "\tDoing DNS... "
    start = get_time()
    try:
        parsed_url.ip = gethostbyname(parsed_url.host)
        if parsed_url.ip is None:
            fail(message)
            return data
    except (Exception, AttributeError):
        fail(message)
        return data
    data['dns_time'] = get_time() - start
    data['dns'] = 1
    message += "done in {}ms, found {}\n".format(get_time() - start, parsed_url.ip)

    # check is IP is unique #
    message += "\tChecking IP uniqueness... "
    if not unique_ip(hash(parsed_url.ip)):
        fail(message)
        return data
    message += "passed\n"

    # create socket and connect to server #
    start_robot = get_time()
    ws = Pysock()
    message += "\tConnecting on robots... "
    start = get_time()
    try:
        ws.connect(parsed_url.ip, parsed_url.port)
    except SocketError:
        fail(message)
        return data
    message += " done in {}ms\n".format(get_time() - start)

    # Send HEAD request #
    try:
        ws.send(head_req.format(parsed_url.host).encode())
    except SocketError:
        return data
    # load response #
    message += "\tLoading robots ... "
    start = get_time()
    try:
        head_resp = ws.receive()
        if len(head_resp) < 10: raise SocketError
    except SocketError:
        fail(message)
        return data
    data['robot_time'] = get_time() - start_robot
    data['robot'] = 1
    message += "done in {}ms with {} bytes\n".format(get_time() - start, len(head_resp))

    # verify header if robot allowed #
    code = head_resp[9:12]
    message += "\tVerifying header... status code {}\n".format(code)
    #data['codes'] = Counter()
    data['code' + head_resp[9]] = 1
    if head_resp[9] is '2':
        # print(message)
        # queue.task_done()
        return data

    ws.sock.close()
    # create new socket and connect #

    start_page = get_time()
    ws = Pysock()
    message += "\tConnecting on page... "
    start = get_time()
    try:
        ws.connect(parsed_url.ip, parsed_url.port)
    except SocketError:
        fail(message)
        return data
    message += "done in {}ms\n".format(get_time() - start)

    # Send the GET request #
    try:
        ws.send(get_req.format(parsed_url.path, parsed_url.query, parsed_url.host).encode())
    except:
        return data
    # load data #
    message += "\tLoading... "
    start = get_time()
    try:
        get_resp = ws.receive()
        if len(get_resp) < 10: raise SocketError
    except SocketError:
        fail(message)
        return data
    data['page_time'] = get_time() - start_page
    data['page'] = 1
    data['page_size'] = len(get_resp)
    message += "done in {}ms with {} bytes\n".format(get_time() - start, len(get_resp))

    # verify header #
    message += "\tVerifying header... status code {}\n".format(get_resp[9:12])
    data['code' + get_resp[9]] = 1

    if get_resp is not None and get_resp[9] is not '2':
        # print(message + "\n" + get_resp)
        # queue.task_done()
        return data

    # parse for urls #
    message += "\tParsing page... "
    start = get_time()
    num_links = len(findall(r'(https?://\S+)', get_resp))
    data['link_time'] = get_time() - start
    data['link'] = num_links
    message += "done in {}ms with {} links\n".format(get_time() - start, num_links)
    # message += "\tdata: " + str(data)
    ws.sock.close()
    # print(message)
    return data


def run():
    while True:
        url = queue.get()
        size = queue.qsize()
        if size != 0 and size % 10000 is 0:
            print('size: ' + str(size) + " " + url)
        data = run_task(url)
        with DATA_LOCK:
            DATA.update(data)
        queue.task_done()


start_time = get_time()

for x in range(1, int(argv[1])+1):
    thread = Thread(target=run)
    thread.daemon = True
    thread.start()

# Try to open file #
try:
    url_count = 0
    start = get_time()
    with open(argv[2]) as file:
        print("Opened " + argv[2])
        for line in file:
            queue.put(line)
            url_count += 1
    print("FINISHED READING IN")
    with DATA_LOCK:
        DATA['urls'] = url_count
        DATA['urls_time'] = get_time() - start
except IOError:
    print('No such file: ' + argv[2] + '\n')
    exit()

# block until all items in queue call task_done() #
queue.join()
print("DATA: " + str(DATA))
print("Extracted {} URLs @ {}/s".format(DATA['urls'], DATA['urls_time']/1000))
print("Looked up {} DNS names @ {}/s".format(DATA['dns'], DATA['dns_time']/1000))
print("Downloaded {} robots @ {}/s".format(DATA['robot'], DATA['robot_time']/1000))
print("Crawled {} pages @ {}/s (1651.63 MB)".format(DATA['page'], DATA['page_time']/1000, DATA['page_size']/10000000))
print("Parsed {} links @ {}/s".format(DATA['link'], DATA['link_time']/10000))
print("HTTP codes: 2xx = {}, 3xx = {}, 4xx = {}, 5xx = {}".format(DATA['code2'], DATA['code3'], DATA['code4'], DATA['code5']))
print("done in {}/s".format((get_time() - start_time))/1000)

