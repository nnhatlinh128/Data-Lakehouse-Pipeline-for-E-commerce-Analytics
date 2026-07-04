FROM apache/superset:latest
USER root
RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install psycopg2-binary
USER superset
