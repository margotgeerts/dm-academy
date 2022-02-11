FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1
USER root

WORKDIR src/
COPY  requirements.txt .

RUN pip3 install -r requirements.txt
COPY  etl.py .

#USER 185 

EXPOSE 8080
ENTRYPOINT ["python3"]
CMD ["etl.py"]
