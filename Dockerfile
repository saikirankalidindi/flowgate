FROM python:3.13-slim
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv
COPY pyproject.toml README.md ./
COPY src ./src
RUN uv pip install --system --no-cache .
EXPOSE 8080
ENTRYPOINT ["flowgate"]
CMD ["serve", "-c", "/etc/flowgate/config.yaml"]
