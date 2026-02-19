FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install -r /tmp/requirements.txt

EXPOSE 8888

CMD ["bash", "-lc", "jupyter lab --ip=0.0.0.0 --port=\"${JUPYTER_PORT:-8888}\" --no-browser --allow-root --ServerApp.allow_remote_access=True --ServerApp.allow_origin='*' --ServerApp.token=\"${JUPYTER_TOKEN:-}\" --ServerApp.password='' --ServerApp.root_dir=\"${JUPYTER_WORKDIR:-/workspace/notebooks}\""]
