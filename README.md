# Project Outline and Description:

https://docs.google.com/document/d/1oksx76JP8RAb4EHxrFCBLhbGqK16XP1FhybWazK4P6E/edit?usp=sharing

## Working with Docker

Once Docker Desktop is installed, run the following command in the root directory:

```
docker compose up
```

If port `5433` already taken, create a `.env` file in the root directory and set `POSTGRES_PORT` to a different number, such as `5432`.

`docker compose exec python manage.py migrate` must be run at the start to initialize Django's database inside of the container.
