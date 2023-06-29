FROM pwalski/golem_nvidia_base:latest

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y bash openssh-server iproute2 tcpdump net-tools screen
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y \
    libglvnd-dev \
    libx11-6 \
    libxi6 \
    libxxf86vm1 \
    libxrender1 \
    libxfixes3 \
    unzip \
    mesa-utils \
    xorg \
    inetutils-syslogd \
    && rm -rf /var/lib/apt/lists/*

RUN cp /usr/lib64/xorg/modules/drivers/nvidia_drv.so /usr/lib/xorg/modules/drivers/
RUN cp /usr/lib64/xorg/modules/extensions/libglxserver_nvidia.so /usr/lib/xorg/modules/extensions/

RUN echo "UseDNS no" >> /etc/ssh/sshd_config && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config
