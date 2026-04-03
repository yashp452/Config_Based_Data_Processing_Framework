FROM bitnami/spark:latest

USER root

WORKDIR /app

# Bundle Delta Lake + PostgreSQL JDBC JARs (hadoop-aws already in image)
COPY jars/ /opt/bitnami/spark/jars/

# Install Python dependencies from pre-downloaded wheels (no internet needed)
COPY wheels/ /tmp/wheels/
RUN pip install --no-cache-dir --no-index --find-links /tmp/wheels \
        pytest pluggy packaging iniconfig importlib-metadata zipp \
    && pip install --no-cache-dir --no-index --find-links /tmp/wheels --no-deps delta-spark \
    && rm -rf /tmp/wheels

# Copy application source
COPY src/ ./src/
COPY schemas/ ./schemas/
COPY tests/ ./tests/
COPY run_pipeline.py ./

ENTRYPOINT ["python", "run_pipeline.py"]
