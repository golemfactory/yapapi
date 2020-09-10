from yapapi.runner import Engine, Task, vm
from yapapi.runner.ctx import WorkContext
from datetime import timedelta

from ffmpeg_tools.codecs import VideoCodec
from ffmpeg_tools.formats import Container

import asyncio
import sys
import os


class TranscodingParams:
    def __init__(self):
        self.format: str = "mp4"
        self.codec: str = "h265"
        self.input: str = ""
        self.resolution = [1920, 1080]


async def main():
    # Path to file to transcode
    video_file: str = sys.argv[1]
    codec: VideoCodec = VideoCodec.from_name(sys.argv[2])

    params = TranscodingParams()
    params.codec = codec.value
    params.input = video_file

    package = await vm.repo(
        image_hash="f30fce4f0f0fcc1a613c89fa6d1edf22de121531df0b2977cb67d714",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker(ctx: WorkContext, tasks):
        async for task in tasks:
            format: str = task.data.format
            filename, ext = os.path.splitext(task.data.input)

            ctx.send_file(task.data.input, f"/golem/resources/input_video{ext}")
            ctx.send_json(
                "/golem/work/params.json",
                {
                    "command": "transcode",
                    "track": f"/golem/resources/input_video{ext}",
                    "targs": {
                        "container": task.data.format,
                        "video": {"codec": task.data.codec,},
                        "resolution": task.data.resolution,
                    },
                    "output_stream": f"/golem/output/output.{format}"
                },
            )
            ctx.run("/golem/scripts/run-ffmpeg.sh")
            ctx.download_file(f"/golem/output/output.{format}", f"output.{format}")
            yield ctx.commit(task)
            # TODO: Check if job results are valid
            # and reject by: task.reject_task(msg = 'invalid file')
            task.accept_task()

        ctx.log("Transcoding finished!")
    
    # TODO make this dynamic, e.g. depending on the size of files to transfer
    # worst-case time overhead for initialization, e.g. negotiation, file transfer etc.
    init_overhead: timedelta = timedelta(minutes=3)

    async with Engine(
        package=package,
        max_workers=1,
        budget=10.0,
        timeout=init_overhead + timedelta(minutes=5),
        subnet_tag="testnet",
    ) as engine:

        async for progress in engine.map(worker, [Task(data=params)]):
            print("progress=", progress)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    try:
        asyncio.get_event_loop().run_until_complete(task)
    except (Exception, KeyboardInterrupt) as e:
        print(e)
        task.cancel()
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.3))
