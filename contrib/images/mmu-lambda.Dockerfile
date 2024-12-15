FROM golang:1.23-bullseye AS builder
USER root
WORKDIR /src/connect-mmu
RUN apt-get update && apt-get install -y curl && apt-get install jq -y
COPY . .

RUN env GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o build/ ./...
RUN make install-sentry

# install slinky v1.0.12
COPY scripts/install_slinky.sh /tmp/
RUN chmod +x /tmp/install_slinky.sh && /tmp/install_slinky.sh

# install a bunch of connect versions. the script will install v2.0.0 and onwards.
COPY scripts/install_all_connects.sh /tmp/
RUN chmod +x /tmp/install_all_connects.sh && /tmp/install_all_connects.sh

FROM public.ecr.aws/lambda/provided:al2
COPY --from=builder /src/connect-mmu/build/mmu /usr/local/bin/
COPY --from=builder /usr/local/bin/slinky /usr/local/bin/
COPY --from=builder /go/bin/sentry /usr/local/bin/
# Copy all connect binaries
COPY --from=builder /usr/local/bin/connect-* /usr/local/bin/
COPY --from=builder /usr/local/bin/connect /usr/local/bin/
# Copy config files
COPY --from=builder /src/connect-mmu/local/* /usr/local/bin/local/
# Copy DataDog Lambda extension
COPY --from=public.ecr.aws/datadog/lambda-extension:latest /opt/. /usr/local/bin/opt/
# symlink slinky -> connect-1.0.12
RUN ln -s /usr/local/bin/slinky /usr/local/bin/connect-1.0.12

EXPOSE 8002

WORKDIR /usr/local/bin/

RUN yum update && yum install ca-certificates -y

ENTRYPOINT ["mmu"]