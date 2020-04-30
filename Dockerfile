FROM golang:1.12-alpine AS builder

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN  apk --update --no-cache add git mercurial subversion bzr ca-certificates
ENV GOPROXY=https://goproxy.io
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a  -o go-mysql-kafka .


FROM alpine:3.10
WORKDIR /usr/local/bin
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN mkdir -p /usr/local/bin/config
COPY --from=builder /app/go-mysql-kafka /usr/local/bin/
COPY --from=builder /app/*.toml /usr/local/bin/
ENTRYPOINT ["go-mysql-kafka"]
