"""
API route handlers for the customer analytics application.
"""

from .customers import router as customers_router
from .products import router as products_router
from .analytics import router as analytics_router
from .health import router as health_router

__all__ = [
    "customers_router",
    "products_router",
    "analytics_router",
    "health_router"
]
