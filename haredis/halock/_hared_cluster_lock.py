"""Distributed lock implementation using Redis."""
import uuid
import redis

class HAredlockCluster(object):
    def __init__(self, hosts, lock_key, expire_time=30):
        self.lock_key = lock_key
        self.expire_time = expire_time
        self.clients = [redis.StrictRedis(host=host, port=6379, db=0) for host in hosts]
        self.lock_identifier = str(uuid.uuid4())

    def acquire(self):
        acquired_locks = 0
        for client in self.clients:
            if client.set(self.lock_key, self.lock_identifier, nx=True, ex=self.expire_time):
                acquired_locks += 1

        # Check if the lock was acquired on the majority of instances
        if acquired_locks > len(self.clients) / 2:
            return True

        # If not, release the lock on all instances and return False
        self.release()
        return False

    def release(self):
        for client in self.clients:
            # Use a Lua script to ensure the lock is only released if the identifier matches
            release_script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """
            client.eval(release_script, 1, self.lock_key, self.lock_identifier)
            
class HAAioredlockAio(object):
    def __init__(self, hosts, lock_key, expire_time=30):
        self.lock_key = lock_key
        self.expire_time = expire_time
        self.clients = [redis.StrictRedis(host=host, port=6379, db=0) for host in hosts]
        self.lock_identifier = str(uuid.uuid4())

    def acquire(self):
        acquired_locks = 0
        for client in self.clients:
            if client.set(self.lock_key, self.lock_identifier, nx=True, ex=self.expire_time):
                acquired_locks += 1

        # Check if the lock was acquired on the majority of instances
        if acquired_locks > len(self.clients) / 2:
            return True

        # If not, release the lock on all instances and return False
        self.release()
        return False

    def release(self):
        for client in self.clients:
            # Use a Lua script to ensure the lock is only released if the identifier matches
            release_script = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """
            client.eval(release_script, 1, self.lock_key, self.lock_identifier)
            
if __name__ == "__main__":
    # List of Redis instances' hosts
    hosts = ['localhost', 'redis2.example.com', 'redis3.example.com']

    # Create a DistributedLock instance
    lock = DistributedLock(hosts, 'my_distributed_lock', expire_time=30)

    # Acquire the lock
    if lock.acquire():
        print('Distributed lock acquired')
        # Perform critical section tasks
        lock.release()
    else:
        print('Could not acquire distributed lock')