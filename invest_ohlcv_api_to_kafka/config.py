from environs import Env
import os
env = Env()

REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_DB = env.int("REDIS_DB", 0)
REDIS_USERNAME = env.str("REDIS_USERNAME", None)
REDIS_PASSWORD = env.str("REDIS_PASSWORD", None)

INVEST_INTERVAL = env.str("INVEST_INTERVAL", "Daily")

ATS_VERBOSE_LOGGING = env.bool("ATS_VERBOSE_LOGGING", True)

class RetryTime:
    def __init__(self):
        self.ServiceUnavailable = 30
        self.FileNotFound = 20
        self.ConfigurationError = 40

    def service_unavailable(self):
        return self.ServiceUnavailable

    def file_not_found(self):
        return self.FileNotFound

    def configuration_error(self):
        return self.ConfigurationError


RETRY_TIME = RetryTime()
