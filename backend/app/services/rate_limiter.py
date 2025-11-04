import time
import threading

class TokenBucket:
    """
    Simple thread-safe token bucket limiter.
    capacity == refill_rate per minute (rpm).
    """
    def __init__(self, rpm: int):
        self.capacity = max(1, rpm)
        self.tokens = self.capacity
        self.refill_rate_per_sec = self.capacity / 60.0
        self.lock = threading.Lock()
        self.last_refill = time.monotonic()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.last_refill = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate_per_sec)
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            # brief sleep to avoid busy-wait
            time.sleep(0.05)
