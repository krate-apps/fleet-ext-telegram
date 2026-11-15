.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/telegram-worker ./cmd/telegram-worker

pack:
	bash scripts/pack.sh dist
