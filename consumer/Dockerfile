FROM golang as build-stage
WORKDIR /go/src/app
COPY go.sum .
COPY go.mod .
RUN go mod download
COPY consumer.go .
ENTRYPOINT ["go", "run", "consumer.go"]