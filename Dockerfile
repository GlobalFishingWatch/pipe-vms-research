FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-pipeline:latest

# Setup local application dependencies
COPY . /opt/project

RUN pip install -r requirements.txt
RUN pip install -e .
