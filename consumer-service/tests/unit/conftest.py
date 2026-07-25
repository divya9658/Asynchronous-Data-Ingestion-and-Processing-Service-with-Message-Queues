import os

# Speed up exponential backoff during unit tests so the retry tests don't
# take several real seconds to run.
os.environ.setdefault("BACKOFF_BASE_SECONDS", "0.01")
os.environ.setdefault("MAX_RETRIES", "3")
