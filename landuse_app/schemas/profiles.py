"""Geojson response models are defined here."""

from enum import Enum


class Profile(str, Enum):
    INDUSTRIAL: str = "Industrial"
    RESIDENTIAL: str = "Residential"
    SPECIAL: str = "Special"
    TRANSPORT: str = "Transport"
    RECREATION: str = "Recreation"
    AGRICULTURE: str = "Agriculture"
    BUSINESS: str = "Business"
