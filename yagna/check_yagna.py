import sys
import urllib.request


def check_me(appkey=""):
    url = "http://127.0.0.1:7465/me"
    headers = {}
    if appkey:
        print(f"Checking yagna connection {url} with appkey {appkey}")
        headers["Authorization"] = f"Bearer {appkey}"
    else:
        print(f"Checking yagna connection {url} without appkey")

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            for h in response.headers:
                print(f" - {h} {response.headers[h]}")
            if response.status == 200:
                print(response.read().decode("utf-8"))
            else:
                print(f"Error: {response.status} {response.text}")
    except urllib.error.URLError as e:
        print(f"Status code: {e.code}")
        print("Headers:")
        for h in e.headers:
            print(f" - {h} {e.headers[h]}")


if sys.argv[1:]:
    check_me(sys.argv[1])
else:
    check_me()

