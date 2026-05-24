ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
COMPOSE_FILE := $(ROOT_DIR)/infra/docker-compose.yml
ENV_FILE := $(ROOT_DIR)/.env

ifeq ($(wildcard $(ENV_FILE)),)
COMPOSE := docker compose -f $(COMPOSE_FILE)
else
COMPOSE := docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE)
endif

.PHONY: up down logs reset build

init:
	mc alias set local http://minio:9000 minioadmin minioadmin

	mc mb local/landing
	mc mb local/processed
	mc mb local/quarantined

	mc event add local/landing arn:minio:sqs::ingestion:webhook --event put

	mc admim config set local notify_webhook:ingestion endpoint=http://host.docker.internal:8080/minio/events \
		queue_limit="10000"

	mc admin service restart local

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
