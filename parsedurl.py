from urllib.parse import urlparse


class UrlParser:
    def __init__(self, link):
        parsed_url = urlparse(link.strip(' \t\n\r'))
        self.ip = ""
        self.host = parsed_url.hostname
        self.port = parsed_url.port
        if self.port is None: self.port = 80
        self.path = parsed_url.path
        if self.path is "": self.path = "/"
        self.query = parsed_url.query
        if self.query is not "": self.query = '?' + self.query