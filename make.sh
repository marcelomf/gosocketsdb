#!/bin/bash
# apt-get install -y libc6-dev libc6-dev-arm64-cross libc6-dev-armel-armhf-cross libc6-dev-armel-cross libc6-dev-armhf-armel-cross libc6-dev-armhf-cross libc6-dev-i386
CGO_ENABLED=1 CC=arm-linux-gnueabi-gcc GOOS=linux GOARCH=arm GOARM=6 go build -v server_unqlite.go
CGO_ENABLED=1 CC=arm-linux-gnueabi-gcc GOOS=linux GOARCH=arm GOARM=6 go build -v client.go
