import os

ENV_GIT_SHA_KEYS = (
    "RAILWAY_GIT_COMMIT_SHA",
    "GIT_SHA",
    "COMMIT_SHA",
    "VERCEL_GIT_COMMIT_SHA",
)


def get_git_sha() -> str | None:
    for key in ENV_GIT_SHA_KEYS:
        value = os.getenv(key)
        if value:
            return value
    return None


def get_build_time() -> str | None:
    build_time = os.getenv("BUILD_TIME")
    return build_time or None
