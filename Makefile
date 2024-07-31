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
	docker save bone-mineral-density-postgres-bmd:latest -o bone-mineral-density-postgres-bmd.tar
	docker save bone-mineral-density-bmd:latest -o bone-mineral-density-bmd.tar

import:
	docker load -i bone-mineral-density-postgres-bmd.tar
	docker load -i bone-mineral-density-bmd.tar
