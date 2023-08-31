#!/bin/sh

# Using the redis-cli tool available as default in the Redis base image
# we need to create the cluster so they can coordinate with each other
# which key slots they need to hold per shard

set -e

# wait a little so we give some time for the Redis containers
# to spin up and be available on the network
sleep 5
# redis-cli doesn't support hostnames, we must match the
# container IP addresses from our docker-compose configuration.
# `--cluster-replicas 1` Will make sure that every master node will have its replica node

echo "List of Redis Instances:"

echo "Redis 1: ${IP_REDIS_1}:${REDIS_1_PORT}"
echo "Redis 2: ${IP_REDIS_2}:${REDIS_2_PORT}"
echo "Redis 3: ${IP_REDIS_3}:${REDIS_3_PORT}"
echo "Redis 4: ${IP_REDIS_4}:${REDIS_4_PORT}"
echo "Redis 5: ${IP_REDIS_5}:${REDIS_5_PORT}"
echo "Redis 6: ${IP_REDIS_6}:${REDIS_6_PORT}"

echo "🚀 Creating Redis cluster..."



echo "yes" | redis-cli --cluster create \
  "${IP_REDIS_1}":"${REDIS_1_PORT}" \
  "${IP_REDIS_2}":"${REDIS_2_PORT}" \
  "${IP_REDIS_3}":"${REDIS_3_PORT}" \
  "${IP_REDIS_4}":"${REDIS_4_PORT}" \
  "${IP_REDIS_5}":"${REDIS_5_PORT}" \
  "${IP_REDIS_6}":"${REDIS_6_PORT}" \
  -a "${REDIS_PASSWORD}" \
  --cluster-replicas "${REPLICA_COUNT}"

echo "🚀 Redis cluster ready."
