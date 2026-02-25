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

    @field_validator("title", "username", "image_url", mode="before")
    @classmethod
    def require_non_empty_str(cls, value, info):
        if value is None:
            raise ValueError(f"{info.field_name} is required")
        string = str(value).strip()
        if not string:
            raise ValueError(f"{info.field_name} is required")
        return string

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
            string = str(value)
        else:
            string = re.sub(r"\D", "", str(value))
        if not string:
            return None
        if string.startswith("0") and len(string) == 10:
            string = "380" + string[1:]
        elif len(string) == 9:
            string = "380" + string
        return int(string)
