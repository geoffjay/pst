all: build

build:
	@go build -o pst main.go

clean:
	@rm pst

.PHONY: all build clean
