# Container image of the CloudVM Karpenter controller; build with the src directory as context so the nodeclaim
# module the controller replaces locally is available.
FROM --platform=$BUILDPLATFORM golang:1.27.2-alpine3.23 AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src/karpenter

COPY nodeclaim /src/nodeclaim
COPY karpenter/go.mod karpenter/go.sum ./
RUN go mod download

COPY karpenter/apis ./apis
COPY karpenter/cloudprovider ./cloudprovider
COPY karpenter/cmd ./cmd

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/cloudvm-karpenter \
    ./cmd/controller

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/cloudvm-karpenter /cloudvm-karpenter

USER 65532:65532
ENTRYPOINT ["/cloudvm-karpenter"]
