# README

Creating docker image

```bash
docker build -t huge-output-command .
```

Testing image using docker

```bash
docker run --rm -it huge-output-command /bin/sh --login
```

Copy input file

```sh
docker ps
docker cp <container-id>:hello-from-golem.txt huge-output-command-input.log
```

Build gvmi and push to repository - push will not work as this exact image is already there

```bash
gvmkit-build huge-output-command
gvmkit-build huge-output-command --push
```

Run

```bash
python output_command.py 
```

Compare files difference in size

```bash
ls -ls huge-output-command-*
```
