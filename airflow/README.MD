Setup process:

  1. Prepare the enviroment by creating the config/ logs/ and plugins/ folders

  2. Export the user_id with `echo -e "AIRFLOW_UID=$(id -u)" > .env`

  3. To build images, use `docker compose build` 

  3. To run setup, first run the set up container using `docker compose up -rm airflow-init`

  4. Then you can start desired containers, but you can just use docker compose up as well 

  5. Log in on http://localhost:8080/ using login: airflow password: airflow

  6. Remember that you need to run the postgres database from ../database/docker-compose.yaml file, in order to get the race_dag running. (We dont mean the default database that airflow uses to store metadata).

  7. If you set up your database and airflow containers, you can run desired dags.


