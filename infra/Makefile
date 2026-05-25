ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
COMPOSE_FILE := $(ROOT_DIR)/infra/docker-compose.yml
ENV_FILE := $(ROOT_DIR)/.env

ifeq ($(wildcard $(ENV_FILE)),)
COMPOSE := docker compose -f $(COMPOSE_FILE)
else
COMPOSE := docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE)
endif

.PHONY: up down logs reset build

init: init-buckets

init-buckets:
	$(COMPOSE) exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
	$(COMPOSE) exec minio mc mb --ignore-existing local/landing
	$(COMPOSE) exec minio mc mb --ignore-existing local/processed
	$(COMPOSE) exec minio mc mb --ignore-existing local/quarantined

init-events:
	$(COMPOSE) exec minio mc admin config set local notify_webhook:ingestion \
		endpoint="http://host.docker.internal:8080/minio/events" \
		queue_limit="10000"
	$(COMPOSE) exec minio mc admin service restart local
	$(COMPOSE) exec minio mc event add local/landing \
		arn:minio:sqs::ingestion:webhook --event put

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=200

reset:
	$(COMPOSE) down -v --remove-orphans

build:
	go build ./...
