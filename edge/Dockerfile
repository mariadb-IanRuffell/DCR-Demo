FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /edge .

FROM alpine:3.21
ARG TARGETARCH
ARG KUBECTL_VERSION=v1.33.0
RUN apk add --no-cache ca-certificates curl && \
    curl -fsSL -o /usr/local/bin/kubectl https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl && \
    chmod +x /usr/local/bin/kubectl && apk del curl
COPY --from=build /edge /usr/local/bin/edge
ENTRYPOINT ["/usr/local/bin/edge"]
