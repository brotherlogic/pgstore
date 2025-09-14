# syntax=docker/dockerfile:1

FROM golang:1.23 AS build

WORKDIR $GOPATH/src/github.com/brotherlogic/pgstore

COPY go.mod ./
#COPY go.sum ./

RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 go build -o /pgstore

##
## Deploy
##
FROM ubuntu:22.04
USER root:root

WORKDIR /
COPY --from=build /pgstore /pgstore

EXPOSE 8080
EXPOSE 8081

ENTRYPOINT ["/pgstore"]