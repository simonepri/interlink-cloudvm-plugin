FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine3.23 AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cloudvm ./cloudvm

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/interlink-provider-cloudvm \
    ./cloudvm

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/interlink-provider-cloudvm /interlink-provider-cloudvm

USER 65532:65532
EXPOSE 4040
ENTRYPOINT ["/interlink-provider-cloudvm"]
