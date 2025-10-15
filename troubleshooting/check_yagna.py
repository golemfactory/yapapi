import sys
import urllib.request

# super simple script for troubleshooting yagna connection
# usage: python check_yagna.py <appkey>


def check_me(appkey):
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
            print("Request successful:")
            print("  Headers:")
            for h in response.headers:
                print(f"   - {h}: {response.headers[h]}")
            if response.status == 200:
                text = response.read().decode("utf-8")
                print(f"  Data:")
                print(f"   - {text}")
            else:
                print(f"  Error: {response.status} {response.text}")
    except urllib.error.HTTPError as e:
        print(f"Request failed: {e.reason}")
        print(f"  Status code: {e.code}")
        print("  Headers:")
        for h in e.headers:
            print(f"   - {h}: {e.headers[h]}")
    except urllib.error.URLError as e:
        print(f"Request failed: {e.reason}")


if sys.argv[1:]:
    check_me(sys.argv[1])
else:
    check_me("")

