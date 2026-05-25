include infra/Makefile

ifneq ($(wildcard .env),)
  include .env
  export
endif
