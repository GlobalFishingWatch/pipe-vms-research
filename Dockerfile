FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-pipeline:latest-python3.8

# Setup local application dependencies
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY . /opt/project
RUN pip install -e .

ENTRYPOINT ["./main.py"]
