# README

## Creating docker image

```bash
docker build -t huge-output-command .
```

### Testing image using docker

```bash
docker run --rm -it huge-output-command /bin/sh --login
```

## Copy output file from docker if you want to verify if it is the same as the one used as input file

```sh
docker ps
docker cp <container-id>:huge-output-command-input.log huge-output-command-output-docker.log
```

## Build gvmi and push to repository - push will not work as this exact image is already there

```bash
gvmkit-build huge-output-command
gvmkit-build huge-output-command --push
```

## Run

```bash
python output_command.py 
```
