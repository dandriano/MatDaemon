FROM matlab_2025 AS runtime

RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && \
    apt-get clean && apt-get -y autoremove && rm -rf /var/lib/apt/lists/*

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
