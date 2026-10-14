"""
Control exposes a Compute over HTTP, on the plyght.web spine
(middleware stack, self-hosted /docs, health probes).

Gives you squeue and scancel style operations from curl:

    GET    /health                  liveness
    GET    /api/v1/jobs             live jobs plus recent history
    GET    /api/v1/jobs/{name}      one job, live or finished
    DELETE /api/v1/jobs/{name}      evict a queued or running job
    GET    /api/v1/resources        capacity totals
    GET    /api/v1/metrics          scrape ready counters and gauges
    GET    /api/v1/progress         progress bar shaped summary

Configuration is ASTEROIDBELT_-prefixed (ASTEROIDBELT_PORT,
ASTEROIDBELT_AUTH_ENABLED, ...) and resources/.env aware. Requires
asteroidbelt[control].
"""

from importlib.metadata import PackageNotFoundError, version

from asteroidbelt.compute.compute import Compute


def release() -> str:
    """
    The installed asteroidbelt version, for the OpenAPI document.
    """
    try:
        return version("asteroidbelt")
    except PackageNotFoundError:
        return "0.0.0"


def build_control(compute: Compute, settings=None):
    """
    Build the FastAPI application wrapping the given compute.
    """
    from fastapi import APIRouter, HTTPException
    from plyght.web import WebSettings, create_app, web_config

    class ControlSettings(WebSettings):
        """ASTEROIDBELT_-prefixed plyght.web settings."""

        model_config = web_config("ASTEROIDBELT")

        name: str = "asteroidbelt"
        version: str = release()

    settings = settings or ControlSettings()
    control = create_app(settings)
    router = APIRouter(tags=["jobs"])

    @router.get("/jobs")
    def jobs() -> dict:
        """Live jobs plus recent history."""
        return {"jobs": compute.jobs()}

    @router.get("/jobs/{name}")
    def job(name: str) -> dict:
        """One job, live or finished."""
        record = compute.find(name)
        if record is None:
            raise HTTPException(status_code=404, detail=f"no job named {name!r}")
        return record.describe()

    @router.delete("/jobs/{name}")
    async def evict(name: str, reason: str = "http") -> dict:
        """Evict a queued or running job."""
        evicted = await compute.evict(name, reason)
        if not evicted:
            raise HTTPException(
                status_code=409,
                detail=f"job {name!r} is not live",
            )
        return {"evicted": name, "reason": reason}

    @router.get("/resources")
    def resources() -> dict:
        """Capacity totals."""
        return compute.resources()

    @router.get("/metrics")
    def metrics() -> dict:
        """Scrape ready counters and gauges."""
        return compute.metrics()

    @router.get("/progress")
    def progress() -> dict:
        """Progress bar shaped summary."""
        return compute.progress()

    control.include_router(router, prefix=settings.api_prefix)
    return control
