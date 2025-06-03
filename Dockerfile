FROM python:3.11-slim
RUN pip install --upgrade uv

WORKDIR /app
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

CMD ["uv", "run", "python", "src/main.py"]
