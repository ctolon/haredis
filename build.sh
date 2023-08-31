#!/usr/bin/env bash
# Example docker builder script for haredis

set -e

docker build -t haredis-base:latest -f ./include/Dockerfile .