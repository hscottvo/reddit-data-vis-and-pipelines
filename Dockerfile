FROM apache/airflow:2.5.0
RUN pip uninstall  --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0
RUN pip install google-api-python-client