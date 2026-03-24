FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /pg2iceberg ./cmd/pg2iceberg

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /pg2iceberg /usr/local/bin/pg2iceberg
ENTRYPOINT ["pg2iceberg"]
