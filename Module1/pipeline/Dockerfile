FROM python:3.13.11-slim
#docker image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin  
# Set working directory
WORKDIR /code
#indicate PATH and is not necesary ENTRYPOINT [ "uv", "run","python", "pipeline.py" ]
ENV PATH="/code/.venv/bin:$PATH"

#Copy dependency files first (better layer caching)
COPY pyproject.toml .python-version uv.lock ./

#Install dependencies files first (better layer caching)
RUN uv sync --locked

#Copy aplication code --RUN pip install pandas pyarrow
COPY pipeline.py .

#Set entry point
#ENTRYPOINT [ "uv", "run","python", "pipeline.py" ]
ENTRYPOINT [ "python", "pipeline.py" ]