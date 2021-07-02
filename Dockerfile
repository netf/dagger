FROM golang:1.16.5
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN go build -o /app/dagger cmd/dagger/main.go
ENTRYPOINT ["/app/dagger"]