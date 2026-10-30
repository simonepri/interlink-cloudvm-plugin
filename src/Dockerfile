FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine3.23 AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY nodeclaim ./nodeclaim
COPY go.mod go.sum ./
RUN go mod download
COPY nodeagent/go.mod nodeagent/go.sum ./nodeagent/
RUN go -C nodeagent mod download

COPY cloudvm ./cloudvm
COPY nodeagent ./nodeagent

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/interlink-cloudvm-plugin \
    ./cloudvm
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go -C nodeagent build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/cloudvm-node-agent \
    .

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/interlink-cloudvm-plugin /interlink-cloudvm-plugin
COPY --from=build /out/cloudvm-node-agent /cloudvm-node-agent

USER 65532:65532
EXPOSE 4040
ENTRYPOINT ["/interlink-cloudvm-plugin"]
