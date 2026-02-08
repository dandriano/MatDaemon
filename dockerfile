FROM mathworks/matlab-deps:r2025a AS base

# Install MATLAB
FROM base AS install-matlab
COPY matlab-install matlab-install
COPY matlab_installer_input.txt .

RUN chmod +x matlab-install/install && \
    matlab-install/install -inputFile matlab_installer_input.txt

# Build runtime environment
FROM base AS build-runtime
COPY --from=install-matlab /opt/matlab /opt/matlab

RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && \
    apt-get clean && apt-get -y autoremove && rm -rf /var/lib/apt/lists/*

RUN ln -s /opt/matlab/bin/matlab /usr/local/bin/matlab && \
    useradd -ms /bin/bash matlab && \
    echo "matlab ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/matlab && \
    chmod 0440 /etc/sudoers.d/matlab

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir aiohttp
RUN pip install /opt/matlab/extern/engines/python

USER matlab

WORKDIR /home/matlab
COPY server.py .
COPY matlab matlab

EXPOSE 80
CMD ["python", "-u", "server.py"]
