# jupyter/Dockerfile
FROM jupyter/pyspark-notebook

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /home/jovyan/work

EXPOSE 8888
