conda env create -f SCRAPE_env.yml

conda install -c conda-forge psycopg2

conda upload_to_postgres.py --mode upsert
