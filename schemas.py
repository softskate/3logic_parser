from pydantic import BaseModel, ConfigDict
from typing import Optional


class ProductSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    brandName: Optional[str] = None
    productId: str
    name: str
    in_stock: str
    price: int
    category: str
    details: dict

