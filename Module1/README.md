# docker workshop
 Workshop Codespaces

 commands:
    - docker run hello-world
    - docker run -it ubuntu
    - docker run -it python:3.13.11-slim
    - docker run -it --entrypoint=bash python:3.13.11-slim
    - docker ps -a
    - docker ps -aq (remove dockers)
    - docker run -it --entrypoint=bash -v $(pwd)/test:/app/test python:3.13.11-slim
    - docker build -t test:pandas  .
    - docker run -it test:pandas
    - docker run -it --entrypoint=bash --rm test:pandas
    - docker run -it test:pandas  12 (with entrypoint)
    - docker run -it --rm test:pandas 12 (with pyproject.toml and uv.lock files)
 test:
    - mkdir test
    - touch file1.txt file2.txt file3.txt
    - echo "Hello from host" > file1.txt
    - python3 script.py 
 pipeline:
    - source .venv/bin/activate.fish
    - deactivate
    - python3 pipeline.py 3
    - snap install astral-uv --classic
    - uv venv --python 3.13
    - uv run python -V
    - uv run which python
    - uv init
    - uv add pyarrow pandas
    - uv add --dev pgli 
    - uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
    - uv add --dev jupyter
    - uv run jupyter notebook
    - uv run jupyter notebook --allow-root (not recommended)
    - uv run jupyter nbconvert --to=script notebook.ipynb
    - uv run python ingest_data.py

 postgres:
    - \dt
    - CREATE TABLE test(id INTEGER, name VARCHAR(50));
    - INSERT INTO test VALUES (1, 'Hello Docker');
    - SELECT * FROM test;
    - \q (to exit)



