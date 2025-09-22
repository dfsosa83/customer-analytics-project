"""
API route handlers for the customer analytics application.
"""

# Import available routers
from .snowflake import router as snowflake_router

# TODO: Add other routers as they are implemented
# from .customers import router as customers_router
# from .products import router as products_router
# from .analytics import router as analytics_router
# from .health import router as health_router

__all__ = [
    "snowflake_router"
    # "customers_router",
    # "products_router",
    # "analytics_router",
    # "health_router",
]
