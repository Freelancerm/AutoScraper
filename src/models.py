import re
from datetime import datetime, UTC
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class CarListing(BaseModel):
    url: str
    title: str
    price_usd: Optional[int] = None
    odometer: Optional[int] = None
    username: str
    phone_number: Optional[int] = None
    image_url: str
    images_count: int
    car_number: Optional[str] = None
    car_vin: Optional[str] = None
    datetime_found: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("price_usd", mode="before")
    @classmethod
    def parse_price(cls, value):
        if isinstance(value, int):
            return value
        s = str(value).strip().lower()
        digits = re.sub(r"\D", "", s)
        return int(digits) if digits else None

    @field_validator("odometer", mode="before")
    @classmethod
    def parse_odometer(cls, value):
        if value is None:
            return None
        if isinstance(value, int):
            return value
        s = str(value).strip().lower()
        if not s:
            return None
        is_thousands = "тис" in s
        digits = re.sub(r"\D", "", s)
        if not digits:
            return None
        return int(digits) * 1000 if is_thousands else int(digits)

    @field_validator("phone_number", mode="before")
    @classmethod
    def parse_phone_number(cls, value):
        if value is None:
            return None
        if isinstance(value, int):
            return value
        s = re.sub(r"\D", "", str(value))
        return int(s) if s else None
