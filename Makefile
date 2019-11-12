### Makefile ---

## Author: Shell.Xu
## Version: $Id: Makefile,v 0.0 2017/01/17 03:44:24 shell Exp $
## Copyright: 2017, Eleme <zhixiang.xu@ele.me>
## License: MIT
## Keywords:
## X-URL:

all: build

build:
	mkdir -p bin
	go build -o bin/influx-proxy github.com/chengshiwen/influx-proxy/service

test:
	rm -rf data/test
	mkdir -p data/test
	go test -v github.com/chengshiwen/influx-proxy/backend

bench:
	rm -rf data/test
	mkdir -p data/test
	go test -bench=. github.com/chengshiwen/influx-proxy/backend

run: build
	bin/influx-proxy -config proxy.json -log-path ""

clean:
	rm -rf bin data


### Makefile ends here
