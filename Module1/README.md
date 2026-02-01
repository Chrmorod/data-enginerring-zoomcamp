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
    - docker build -t taxi_ingest:v001 .
    - docker network connect pg-network elegant_tesla (previous docker -ps)
    - docker run -it --rm \
      --network=pg-network \
      taxi_ingest:v001 \
           --year=2021 \
           --month=1 \
           --pg_user=root \
           --pg_pass=root \
           --pg_host=elegant_tesla \
           --pg_port=5432 \
           --pg_db=ny_taxi \
           --target_table=yellow_taxi_trips \
           --chunksize=100000

    - docker run -it \
      -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
      -e PGADMIN_DEFAULT_PASSWORD="root" \
      -v pgadmin_data: /var/lib/pgadmin \
      -p 8085:80 \
      --network=pg-network \
      --name pgadmin \
      dpage/pgadmin4
      
      docker-compose down -v
      docker-compose up -d pgdatabase pgadmin
      docker-compose run --rm taxi_ingest

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
    - uv run python ingest_data.py --help
    - uv run python ingest_data.py --year 2022 --month 6 --pg_user root --chunksize 50000
    - uv run python ingest_data.py --year=2021 \
                                    --month=1 \
                                    --pg_user=root \
                                    --pg_pass=root \
                                    --pg_host=localhost \
                                    --pg_port=5432 \
                                    --pg_db=ny_taxi \
                                    --target_table=yellow_taxi_trips \
                                    --chunksize=100000

 postgres:
    - \dt
    - CREATE TABLE test(id INTEGER, name VARCHAR(50));
    - INSERT INTO test VALUES (1, 'Hello Docker');
    - SELECT * FROM test;
    - \q (to exit)



