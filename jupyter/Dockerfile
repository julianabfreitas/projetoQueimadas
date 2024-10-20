# jupyter/Dockerfile
FROM jupyter/pyspark-notebook

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Definir o diretório de trabalho
WORKDIR /home/jovyan/work
# Expor a porta do Jupyter Notebook
EXPOSE 8888
