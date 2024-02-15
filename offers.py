import json

f = open("offers.txt", "r")

RUNTIME = "golem.runtime.name"

runtimes = {}

for l in f.readlines():
    props = json.loads(l)
    runtimes.setdefault(props.get(RUNTIME), props)

print(json.dumps(runtimes, indent=4))