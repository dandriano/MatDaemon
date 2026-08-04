FROM localhost:5000/matlab_2025 AS runtime

RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && \
    apt-get clean && apt-get -y autoremove && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir aiohttp dotenv
RUN pip install /opt/matlab/extern/engines/python

WORKDIR /matdaemon
COPY .env.example .env
COPY matlab matlab
COPY backend backend
COPY frontend frontend
COPY *.py .
 
EXPOSE 8080
CMD ["python", "-u", "main.py"]
