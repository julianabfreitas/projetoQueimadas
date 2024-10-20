# minio/Dockerfile
FROM minio/minio

# Expor a porta do Minio
EXPOSE 9000
EXPOSE 9001

# Comando para iniciar o servidor Minio
ENTRYPOINT ["minio"]
CMD ["server", "/data"]

