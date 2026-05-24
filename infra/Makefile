ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
COMPOSE_FILE := $(ROOT_DIR)/infra/docker-compose.yml
ENV_FILE := $(ROOT_DIR)/.env

ifeq ($(wildcard $(ENV_FILE)),)
COMPOSE := docker compose -f $(COMPOSE_FILE)
else
COMPOSE := docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE)
endif

.PHONY: up down logs reset build

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