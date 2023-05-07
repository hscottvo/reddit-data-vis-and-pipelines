FROM apache/airflow:2.5.0
RUN pip uninstall  --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0
RUN pip install google-api-python-client
RUN pip install google-auth-oauthlib
RUN pip install python-dotenv
RUN pip install --upgrade praw
RUN pip install SQLAlchemy
RUN pip install sqlalchemy-cockroachdb
RUN pip install psycopg2-binary
RUN pip install ratelimit
RUN pip install dbt-postgres
RUN curl --create-dirs -o $HOME/.postgresql/root.crt 'https://cockroachlabs.cloud/clusters/dde00dd7-575b-4861-9b7f-2de92729440d/cert'

# Default powerline10k theme, no plugins installed
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.4/zsh-in-docker.sh)"