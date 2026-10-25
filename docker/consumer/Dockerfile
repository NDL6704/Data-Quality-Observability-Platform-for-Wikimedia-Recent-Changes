FROM python:3.12-slim
WORKDIR /workspace
COPY pyproject.toml README.md ./
COPY src ./src
COPY schemas ./schemas
COPY dashboards ./dashboards
COPY scripts ./scripts
RUN pip install --no-cache-dir .
CMD ["python", "-m", "src.processing.consumer", "--workers", "3"]
