from fastapi import APIRouter

from . import health, scores

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(scores.router)

__all__ = ["api_router"]
