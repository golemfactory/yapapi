FROM golemfactory/ffmpeg-experimental:0.104.1
COPY run-ffmpeg.sh /golem/scripts/
VOLUME /golem/work /golem/output /golem/resources
