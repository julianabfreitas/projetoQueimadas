# jupyter/Dockerfile
FROM jupyter/pyspark-notebook

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Dependências MinIO
COPY jars/aws-java-sdk-bundle-1.12.262.jar ${SPARK_HOME}/jars/
COPY jars/hadoop-aws-3.3.4.jar ${SPARK_HOME}/jars/

WORKDIR /home/jovyan/work

EXPOSE 8888