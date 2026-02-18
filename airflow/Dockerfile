FROM apache/airflow:2.8.0

# Install Java for Spark (required by PySpark)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Install dependencies as the airflow user
USER airflow
RUN pip install --no-cache-dir \
    pyspark \
    delta-spark \
    pandas \
    requests \
    python-dotenv \
    pydantic \
    pydantic-settings \
    pytz \
    acryl-datahub \
    acryl-datahub-airflow-plugin \
    boto3 \
    psycopg2-binary \
    opentelemetry-api \
    opentelemetry-sdk \
    opentelemetry-exporter-otlp \
    apache-airflow-providers-fab \
    apache-airflow-providers-openlineage>=1.8.0 \
    authlib

# Force upgrade to compatible openlineage provider for Airflow >= 2.10
RUN pip install --no-cache-dir --upgrade "apache-airflow-providers-openlineage>=1.8.0"

# Bake DAGs and project code into the image so Kubernetes clusters do not need hostPath mounts.
USER root
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
COPY --chown=airflow:root pipelines/ /opt/airflow/project/pipelines/
COPY --chown=airflow:root shared/ /opt/airflow/project/shared/
COPY --chown=airflow:root scripts/ /opt/airflow/project/scripts/
COPY --chown=airflow:root dbt_parallel/ /opt/airflow/project/dbt_parallel/
COPY --chown=airflow:root schema/ /opt/airflow/project/schema/
COPY --chown=airflow:root pyproject.toml /opt/airflow/project/pyproject.toml
COPY --chown=airflow:root setup.cfg /opt/airflow/project/setup.cfg
COPY --chown=airflow:root airflow/webserver_config.py /opt/airflow/webserver_config.py

RUN mkdir -p /opt/airflow/logs \
    && chown -R airflow:root /opt/airflow/logs /opt/airflow/project /opt/airflow/dags /opt/airflow/plugins

USER airflow
