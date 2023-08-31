#!/usr/bin/env bash
# Example docker builder script for haredis

set -e
 
docker build -t haredis-base:latest --build-arg PYTHON_VERSİON=3.8-bullseye -f ./include/Dockerfile . 