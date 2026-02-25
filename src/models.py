from datetime import datetime
from pydantic import BaseModel, Field


class CarListing(BaseModel):
    url: str
    title: str
    price_usd: int
    odometer: int
    username: str
    phone_number: int
    image_url: str
    images_count: int
    car_number: str
    car_vin: str
    datetime_found: datetime = Field(default_factory=datetime.now)
