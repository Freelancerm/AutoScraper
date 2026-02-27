from datetime import datetime, UTC
from pydantic import BaseModel, Field


class CarListing(BaseModel):
    url: str
    title: str
    price_usd: int | None
    odometer: int | None
    username: str
    phone_number: int | None
    image_url: str | None
    images_count: int
    car_number: str | None
    car_vin: str | None
    datetime_found: datetime = Field(default_factory=lambda: datetime.now(UTC))
