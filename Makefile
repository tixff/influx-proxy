### Makefile ---


export GO_BUILD=GO111MODULE=on CGO_ENABLED=0 go build -o bin/influx-proxy -ldflags "-s -w -X main.GitCommit=$(shell git rev-parse --short HEAD) -X 'main.BuildTime=$(shell date '+%Y-%m-%d %H:%M:%S')'"

.PHONY: build linux test bench run lint down tidy clean

all: build

build: lint
	$(GO_BUILD)

linux: lint
	GOOS=linux GOARCH=amd64 $(GO_BUILD)

test:
	go test -v github.com/tixff/influx-proxy/backend

bench:
	go test -bench=. -run=none github.com/tixff/influx-proxy/backend

run:
	go run main.go

lint:
	golangci-lint run --enable=golint --disable=errcheck --disable=typecheck && goimports -l -w . && go fmt ./... && go vet ./...

down:
	go list ./... && go mod verify

tidy:
	head -n 3 go.mod > go.mod.tmp && mv go.mod.tmp go.mod && rm -f go.sum && go mod tidy -v

clean:
	rm -rf bin data log

### Makefile ends here
