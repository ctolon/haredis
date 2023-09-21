#!/bin/bash

set -e

docker run -it -d --name redis-standalone -p 6379:6379 redis redis-server --appendonly yes --requirepass examplepass