FROM registry.indexexchange.com:5000/ubuntu:18.04 AS build
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y wget
ENV GO_INSTALLER=go1.16.4.linux-amd64.tar.gz
RUN cd /tmp && \
    wget https://dl.google.com/go/$GO_INSTALLER && \
    tar -C /usr/local -xzf $GO_INSTALLER
RUN mkdir -p /app/prebid-cache/
WORKDIR /app/prebid-cache/
ENV GOROOT=/usr/local/go
ENV PATH=$GOROOT/bin:$PATH
ENV GOPROXY="https://proxy.golang.org"
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
ENV CGO_ENABLED 0
COPY ./ ./
RUN go mod vendor
RUN go mod tidy
ARG TEST="true"
RUN if [ "$TEST" != "false" ]; then ./validate.sh ; fi
RUN go build -mod=vendor .

FROM registry.indexexchange.com:5000/ubuntu:18.04 AS release
LABEL maintainer="hans.hjort@xandr.com" 
RUN apt-get update && \
    apt-get install --assume-yes apt-utils && \
    apt-get install -y ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
WORKDIR /usr/local/bin/
COPY --from=build /app/prebid-cache/prebid-cache .
COPY --from=build /app/prebid-cache/config.yaml .
EXPOSE 2424
EXPOSE 2525
ENTRYPOINT ["/usr/local/bin/prebid-cache"]
