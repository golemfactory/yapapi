from http.client import HTTPConnection

h = HTTPConnection("127.0.0.1")
h.request("GET", "/")
with open("/golem/out/test", "w") as f:
    f.write(h.getresponse().read())
