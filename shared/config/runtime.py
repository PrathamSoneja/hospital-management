"""
Runtime profile and configuration validation shared by service entrypoints.

Profiles: `test` (pytest only), `local` (laptop with Docker PostgreSQL/emulators),
and the cloud profiles `staging`/`production` (GKE). Cloud profiles reject
emulators, fakes, loopback endpoints, SQLite, and exported service-account key
files; GKE pods authenticate with Workload Identity and receive secrets from
Secret Manager. Error messages name variables, never their values.
"""
import os
from urllib.parse import urlsplit

LOCAL_PROFILES = {"test", "local"}
CLOUD_PROFILES = {"staging", "production"}
_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0", "host.docker.internal"}


class ConfigError(RuntimeError):
    pass


def runtime_profile(env=None) -> str:
    env = os.environ if env is None else env
    profile = env.get("ENVIRONMENT", "production").strip().lower()
    if profile not in LOCAL_PROFILES | CLOUD_PROFILES:
        raise ConfigError(f"ENVIRONMENT must be one of {sorted(LOCAL_PROFILES | CLOUD_PROFILES)}")
    return profile


def _endpoint_host(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if "://" in value:
        return urlsplit(value).hostname
    # gRPC targets: host:port, dns:///host:port, or [ipv6]:port
    value = value.split("///", 1)[-1]
    if value.startswith("["):
        return value[1:].split("]", 1)[0]
    return value.rsplit(":", 1)[0] if ":" in value else value


def validate_cloud_runtime(endpoint_names, env=None) -> str:
    """Raise ConfigError when a cloud profile carries local-only configuration."""
    env = os.environ if env is None else env
    profile = runtime_profile(env)
    if profile in LOCAL_PROFILES:
        return profile
    problems = []
    for name, value in env.items():
        if name.endswith("_EMULATOR_HOST") and value.strip():
            problems.append(f"{name} (emulators are local-only)")
    if env.get("USE_FAKES", "").strip():
        problems.append("USE_FAKES (no fake runtime is supported)")
    if env.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip():
        problems.append("GOOGLE_APPLICATION_CREDENTIALS (use Workload Identity, not key files)")
    if env.get("DATABASE_URL", "").strip().lower().startswith("sqlite"):
        problems.append("DATABASE_URL (SQLite is test-only)")
    for name in endpoint_names:
        host = _endpoint_host(env.get(name, ""))
        if host is None:
            problems.append(f"{name} (required)")
        elif host.lower() in _LOOPBACK_HOSTS or host.startswith("127."):
            problems.append(f"{name} (loopback/local demo endpoint)")
    if problems:
        raise ConfigError(f"Invalid {profile} configuration: " + "; ".join(sorted(problems)))
    return profile


def secret_setting(name: str, env=None) -> str:
    """Read NAME from NAME_FILE (a Secret Manager CSI mount) if set, else from NAME."""
    env = os.environ if env is None else env
    path = env.get(f"{name}_FILE", "").strip()
    if path:
        try:
            with open(path, encoding="utf-8") as handle:
                return handle.read().strip()
        except OSError as error:
            raise ConfigError(f"{name}_FILE is set but the mounted secret cannot be read") from error
    return env.get(name, "").strip()


def bounded_int(name: str, default: int, minimum: int, maximum: int, env=None) -> int:
    env = os.environ if env is None else env
    raw = env.get(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError as error:
        raise ConfigError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
    return value
