from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


Biomarker = Literal["GLU", "ALT", "CREA"]


class CreateOrderRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    patient_id: str = Field(min_length=1, max_length=128)
    barcode: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    biomarkers: list[Biomarker] = Field(min_length=1)

    @field_validator("biomarkers")
    @classmethod
    def unique_biomarkers(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("Біомаркери не повинні повторюватися")
        return value


class OrderData(CreateOrderRequest):
    order_id: str = Field(min_length=1)
    status: Literal["SAMPLE_COLLECTED"]


class OrderLookup(BaseModel):
    order: OrderData
    handled_by_order_instance: str
    trace_chain: str


class AssayRunRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)
    barcode: str = Field(default="CODE128-1001", pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    initial_message: str = Field(default="Запит від біохімічного аналізатора", max_length=1000)


class Measurement(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    biomarker: Biomarker
    optical_density: float = Field(ge=0)
    value: float = Field(ge=0)
    unit: str


class AssayCompletedEvent(BaseModel):
    event_type: Literal["AssayCompletedEvent"] = "AssayCompletedEvent"
    event_id: str = Field(min_length=1)
    occurred_at: datetime
    barcode: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    order_instance: str
    assay_instance: str
    measurements: list[Measurement] = Field(min_length=1)
    trace_chain: str
    simulation: Literal[True] = True

    @field_validator("occurred_at")
    @classmethod
    def timezone_required(cls, value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Час події повинен містити часовий пояс")
        return value

    @field_validator("measurements")
    @classmethod
    def unique_measurements(cls, value):
        codes = [item.biomarker for item in value]
        if len(codes) != len(set(codes)):
            raise ValueError("Результати біомаркерів не повинні повторюватися")
        return value
