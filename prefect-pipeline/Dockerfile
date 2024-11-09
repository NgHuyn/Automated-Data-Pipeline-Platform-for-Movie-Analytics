ARG IMAGE_VARIANT=slim-buster
ARG PYTHON_VERSION=3.12.4

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

COPY --from=py3 / /

WORKDIR /opt/prefect-pipeline
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect-pipeline/flows

CMD ["python", "flows/pipeline_1.py"]