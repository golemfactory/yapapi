FROM golemfactory/blender:1.13
COPY run-blender.sh /golem/entrypoints/
VOLUME /golem/work /golem/output /golem/resource
