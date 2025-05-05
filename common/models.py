# common/models.py
from pydantic import BaseModel, Field, field_validator, HttpUrl
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime
import uuid

class RoastLevel(str, Enum):
    LIGHT = "light"
    LIGHT_MEDIUM = "light-medium"  # For those in-between roasts
    MEDIUM = "medium"
    MEDIUM_DARK = "medium-dark"
    DARK = "dark"
    CITY = "city"           # Specialty coffee terminology
    CITY_PLUS = "city-plus" # Slightly darker than City
    FULL_CITY = "full-city" # Medium-dark in specialty terms
    FRENCH = "french"       # Very dark, oil on surface
    ITALIAN = "italian"     # Darkest common roast
    CINNAMON = "cinnamon"   # Very light roast
    FILTER = "filter"       # Some roasters use this term
    ESPRESSO = "espresso"   # Not technically a roast level but commonly used
    OMNIROAST = "omniroast" # For "all-purpose" roasts
    UNKNOWN = "unknown"     # Fixed from "medium" default

class BeanType(str, Enum):
    ARABICA = "arabica"
    ROBUSTA = "robusta"
    LIBERICA = "liberica"  # Much rarer but exists in India
    BLEND = "blend"
    MIXED_ARABICA = "mixed-arabica"  # For blends of different arabica varieties
    ARABICA_ROBUSTA = "arabica-robusta"  # Common blend type in India
    UNKNOWN = "unknown"

class ProcessingMethod(str, Enum):
    WASHED = "washed"
    NATURAL = "natural"
    HONEY = "honey"
    PULPED_NATURAL = "pulped-natural"
    ANAEROBIC = "anaerobic"
    MONSOONED = "monsooned"  # Essential for Indian coffee!
    WET_HULLED = "wet-hulled"
    CARBONIC_MACERATION = "carbonic-maceration"
    DOUBLE_FERMENTED = "double-fermented"
    UNKNOWN = "unknown"  # Fixed from "washed" default

class RoasterModel(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    slug: str
    description: Optional[str] = None
    website_url: HttpUrl
    social_links: Optional[List[HttpUrl]] = Field(default_factory=list)
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    founded_year: Optional[int] = None
    instagram_handle: Optional[str]
    logo_url: Optional[HttpUrl] = None
    has_subscription: bool = False
    has_physical_store: bool = False
    is_featured: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator('founded_year', mode='before')
    def validate_founded_year(cls, value):
        if value is None:
            return value
        try:
            year = int(value)
            current_year = datetime.now().year
            if not (1800 < year <= current_year):
                raise ValueError(f"Founded year {year} must be between 1801 and {current_year}")
            return year
        except (ValueError, TypeError):
            raise ValueError(f"Invalid year format: {value}")

    class Config:
        use_enum_values = True

class CoffeeModel(BaseModel):
    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    roaster_id: str
    name: str
    slug: str
    description: Optional[str] = None
    roast_level: RoastLevel = RoastLevel.UNKNOWN
    bean_type: BeanType = BeanType.UNKNOWN
    processing_method: ProcessingMethod = ProcessingMethod.UNKNOWN
    image_url: Optional[HttpUrl] = None
    direct_buy_url: Optional[HttpUrl] = None
    region_id: Optional[str] = None
    is_seasonal: bool = False
    is_available: bool = True
    is_featured: bool = False
    is_single_origin: bool = False
    tags: Optional[List[str]] = Field(default_factory=list)
    deepseek_enriched: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Additional fields not in DB but used during processing
    region_name: Optional[str] = None
    flavor_profiles: Optional[List[str]] = Field(default_factory=list)
    brew_methods: Optional[List[str]] = Field(default_factory=list)
    external_links: Optional[Dict[str, HttpUrl]] = Field(default_factory=dict)
    prices: Optional[Dict[int, float]] = Field(default_factory=dict)  # size_grams -> price

    @field_validator('prices', mode='before')
    def validate_prices(cls, value):
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise TypeError("Prices must be a dictionary")
        validated_prices = {}
        for k, v in value.items():
            try:
                grams = int(k)
                price = float(v)
                if grams <= 0:
                    raise ValueError(f"Weight (grams) must be positive: {k}")
                if price < 0:
                    raise ValueError(f"Price must be non-negative: {v}")
                validated_prices[grams] = price
            except (ValueError, TypeError):
                raise ValueError(f"Invalid price entry: {k}={v}")
        return validated_prices

    class Config:
        use_enum_values = True