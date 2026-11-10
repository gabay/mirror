#!/bin/bash

docker buildx build -t gabay/mirror --platform linux/amd64,linux/arm64 --push .
