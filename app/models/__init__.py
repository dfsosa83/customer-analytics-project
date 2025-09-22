"""
Data models and schemas for the customer analytics application.
"""

from .customer import Customer, CustomerBase, CustomerCreate, CustomerUpdate
from .product import Product, ProductBase, ProductCreate, ProductUpdate
from .analytics import AnalyticsResult, CustomerProductRelation, AnalyticsQuery

__all__ = [
    "Customer",
    "CustomerBase", 
    "CustomerCreate",
    "CustomerUpdate",
    "Product",
    "ProductBase",
    "ProductCreate", 
    "ProductUpdate",
    "AnalyticsResult",
    "CustomerProductRelation",
    "AnalyticsQuery"
]
