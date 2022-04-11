from environs import Env

env = Env()

REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_DB = env.int("REDIS_DB", 0)
REDIS_USERNAME = env.str("REDIS_USERNAME", None)
REDIS_PASSWORD = env.str("REDIS_PASSWORD", None)

INVEST_INTERVAL = env.str("INVEST_INTERVAL", "Daily")
MAX_PAUSE_BETWEEN_REQUESTS = env.int("MAX_PAUSE_BETWEEN_REQUESTS", 5)

ATS_VERBOSE_LOGGING = env.bool("ATS_VERBOSE_LOGGING", False)

RUNNING_IN_CONTAINER = env.bool("RUNNING_IN_CONTAINER", False)

KAFKA_BROKER = env.str("KAFKA_BROKER")
KAFKA_BOOTSTRAP_SERVERS = KAFKA_BROKER
KAFKA_CONFIGURATION = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    # 'client.id': socket.gethostname(),
    "socket.timeout.ms": 10000,
    "api.version.request": "false",
}

HOSTNAMEY = env.str("HOSTNAMEY", None)
HOSTNAMEALIAS2 = env.str("HOSTNAMEALIAS2", None)
if not HOSTNAMEALIAS2 and HOSTNAMEY:
    HOSTNAMEALIAS2 = HOSTNAMEY


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
