FROM ubuntu:trusty

ENV DEBIAN_FRONTEND noninteractive
ENV GOLANG_VERSION 1.5.2
ENV GOLANG_DOWNLOAD_URL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA1 46eecd290d8803887dec718c691cc243f2175fe0

ENV GOPATH /usr/share/gocode
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN apt-get update -qq && apt-get dist-upgrade -qq -y \
  && apt-get install -qq --no-install-recommends -y \
    build-essential ubuntu-dev-tools debhelper devscripts curl \
    libdistro-info-perl git dh-golang

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& echo "$GOLANG_DOWNLOAD_SHA1  golang.tar.gz" | sha1sum -c - \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
