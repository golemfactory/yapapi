This directory contains files used to construct the application Docker image
that's then converted to a GVMI file (a Golem Virtual Machine Image file) and uploaded
to the Yagna image repository.

All Python scripts here are run within a VM on the Provider's end.

The example (`../simple_service.py`) already contains the appropriate image hash
but if you'd like to experiment with it, feel free to re-build it.

## Building the image

You'll need:

- Docker: https://www.docker.com/products/docker-desktop
- gvmkit-build: `pip install gvmkit-build`

Once you have those installed, run the following from this directory:

```bash
docker build -f simple_service.Dockerfile -t simple-service .
gvmkit-build simple-service:latest
gvmkit-build simple-service:latest --push
```

Note the hash link that's presented after the upload finishes.

e.g. `b742b6cb04123d07bacb36a2462f8b2347b20c32223c1ac49664635f`

and update the service's `get_payload` method to point to this image:

```python
    async def get_payload():
        return await vm.repo(
        image_hash="b742b6cb04123d07bacb36a2462f8b2347b20c32223c1ac49664635f",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )
```
