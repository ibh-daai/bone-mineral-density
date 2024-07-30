up:
	docker compose --env-file .env up --build 

upd:
	docker compose --env-file .env up --build -d

build:
	DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose --env-file .env up --build

stop:
	docker compose down

start:
	docker compose start

reset:
	docker compose down --volumes --remove-orphans

export:
	docker save bmd:latest -o bmd.tar

import:
	docker load -i bmd.tar
