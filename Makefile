.PHONY: build clean

build :
	mkdir -p dist
	env GOOS=linux GOARCH=amd64 go build -o dist/kafkaReaderForCentos ./cmd/kafkaReader
	go build -o dist/kafkaReader ./cmd/kafkaReader

clean :
	rm -rf dist/kafkaReader*
