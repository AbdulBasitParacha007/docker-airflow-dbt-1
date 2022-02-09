FROM apache/airflow:2.2.1-python3.8

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
USER root
#RUN apt-get update && apt-get install -y --no-install-recommends \
 #   libpq-dev python-dev \
  #  unixodbc-dev gcc g++ \
   # git-all


# Airflow
ARG AIRFLOW_USER_HOME=/usr/local/airflow
# ARG AIRFLOW_DEPS=""
# ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV _AIRFLOW_DB_UPGRADE="true"
ENV _AIRFLOW_WWW_USER_CREATE="true"
ENV _AIRFLOW_WWW_USER_PASSWORD="admin"

COPY script/entrypoint.sh /entrypoint.sh
RUN mkdir ${AIRFLOW_USER_HOME}
RUN chown -R airflow: ${AIRFLOW_USER_HOME}
RUN chmod +x /entrypoint.sh
EXPOSE 8080

USER airflow
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

COPY ./script/ ./script/
COPY ./dbt-requirements.txt ./dbt-requirements.txt
USER root 
RUN chmod a+x ./script/dbt_install.sh
RUN ./script/dbt_install.sh ./dbt-requirements.txt
USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]

