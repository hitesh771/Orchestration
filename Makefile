# Mini-K8s (Nova) — build, test, and run targets.
GO      ?= go
BINDIR  ?= bin
PKGS    := ./...

.PHONY: all build test vet fmt clean redis run-api run-agent

all: fmt vet build test

build:
	@mkdir -p $(BINDIR)
	$(GO) build -o $(BINDIR)/api-server ./cmd/api-server
	$(GO) build -o $(BINDIR)/node-agent ./cmd/node-agent
	$(GO) build -o $(BINDIR)/dashboard  ./cmd/dashboard

test:
	$(GO) test $(PKGS)

vet:
	$(GO) vet $(PKGS)

fmt:
	$(GO) fmt $(PKGS)

clean:
	rm -rf $(BINDIR)

# Redis must run with expired-key notifications enabled for the Health Controller.
redis:
	redis-server --port 6379 --notify-keyspace-events Ex --save '' --daemonize yes

run-api: build
	$(BINDIR)/api-server

run-agent: build
	$(BINDIR)/node-agent
