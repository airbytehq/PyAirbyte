FROM docker.io/airbyte/python-connector-base:4.0.2@sha256:9fdb1888c4264cf6fee473649ecb593f56f58e5d0096a87ee0b231777e2e3e73

WORKDIR /airbyte/integration_code

COPY pyproject.toml uv.lock ./
COPY airbyte ./airbyte

RUN pip install --no-cache-dir .

ENV AIRBYTE_ENTRYPOINT="destination-pyairbyte"
ENTRYPOINT ["destination-pyairbyte"]
