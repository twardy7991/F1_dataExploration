Setup process:

  1. Prepare the enviroment by creating the config/ logs/ and plugins/ folders

  2. Export the user_id with `echo -e "AIRFLOW_UID=$(id -u)" > .env'`

  3. To build images, use `docker compose build` 

  3. To run setup, first run the set up container using `docker compose up airflow-init`

  4. Then you can start desired containers