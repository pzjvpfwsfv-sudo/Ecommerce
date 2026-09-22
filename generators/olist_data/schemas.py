from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Literal, Mapping


DATASET_ID = "olist-brazilian-ecommerce-v2"
SCHEMA_VERSION = 1
OFFICIAL_SOURCE_URL = "https://www.kaggle.com/olistbr/brazilian-ecommerce/home"

KNOWN_ORDER_STATUSES = frozenset(
    {
        "created",
        "approved",
        "invoiced",
        "processing",
        "shipped",
        "delivered",
        "unavailable",
        "canceled",
    }
)
KNOWN_PAYMENT_TYPES = frozenset(
    {"credit_card", "boleto", "voucher", "debit_card", "not_defined"}
)

FieldKind = Literal["id", "text", "zip", "uint", "decimal", "coordinate", "timestamp"]


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: FieldKind
    required: bool = False
    minimum: Decimal | None = None


@dataclass(frozen=True)
class TableSpec:
    entity: str
    source_file: str
    fields: tuple[FieldSpec, ...]
    key_fields: tuple[str, ...]
    target_table: str

    @property
    def headers(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.fields)


def _field(
    name: str,
    kind: FieldKind,
    *,
    required: bool = False,
    minimum: int | str | Decimal | None = None,
) -> FieldSpec:
    return FieldSpec(
        name=name,
        kind=kind,
        required=required,
        minimum=None if minimum is None else Decimal(str(minimum)),
    )


def _table(
    entity: str,
    source_file: str,
    fields: tuple[FieldSpec, ...],
    key_fields: tuple[str, ...],
) -> TableSpec:
    return TableSpec(
        entity=entity,
        source_file=source_file,
        fields=fields,
        key_fields=key_fields,
        target_table=f"{entity}_src_v1",
    )


_TABLE_SPECS = {
    "orders": _table(
        "orders",
        "olist_orders_dataset.csv",
        (
            _field("order_id", "id", required=True),
            _field("customer_id", "id", required=True),
            _field("order_status", "text", required=True),
            _field("order_purchase_timestamp", "timestamp", required=True),
            _field("order_approved_at", "timestamp"),
            _field("order_delivered_carrier_date", "timestamp"),
            _field("order_delivered_customer_date", "timestamp"),
            _field("order_estimated_delivery_date", "timestamp"),
        ),
        ("order_id",),
    ),
    "order_items": _table(
        "order_items",
        "olist_order_items_dataset.csv",
        (
            _field("order_id", "id", required=True),
            _field("order_item_id", "uint", required=True, minimum=1),
            _field("product_id", "id", required=True),
            _field("seller_id", "id", required=True),
            _field("shipping_limit_date", "timestamp", required=True),
            _field("price", "decimal", required=True, minimum=0),
            _field("freight_value", "decimal", required=True, minimum=0),
        ),
        ("order_id", "order_item_id"),
    ),
    "order_payments": _table(
        "order_payments",
        "olist_order_payments_dataset.csv",
        (
            _field("order_id", "id", required=True),
            _field("payment_sequential", "uint", required=True, minimum=1),
            _field("payment_type", "text", required=True),
            _field("payment_installments", "uint", required=True, minimum=0),
            _field("payment_value", "decimal", required=True, minimum=0),
        ),
        ("order_id", "payment_sequential"),
    ),
    "order_reviews": _table(
        "order_reviews",
        "olist_order_reviews_dataset.csv",
        (
            _field("review_id", "id"),
            _field("order_id", "id", required=True),
            _field("review_score", "uint", minimum=0),
            _field("review_comment_title", "text"),
            _field("review_comment_message", "text"),
            _field("review_creation_date", "timestamp"),
            _field("review_answer_timestamp", "timestamp"),
        ),
        ("source_row_id",),
    ),
    "customers": _table(
        "customers",
        "olist_customers_dataset.csv",
        (
            _field("customer_id", "id", required=True),
            _field("customer_unique_id", "id", required=True),
            _field("customer_zip_code_prefix", "zip"),
            _field("customer_city", "text"),
            _field("customer_state", "text"),
        ),
        ("customer_id",),
    ),
    "products": _table(
        "products",
        "olist_products_dataset.csv",
        (
            _field("product_id", "id", required=True),
            _field("product_category_name", "text"),
            _field("product_name_lenght", "uint", minimum=0),
            _field("product_description_lenght", "uint", minimum=0),
            _field("product_photos_qty", "uint", minimum=0),
            _field("product_weight_g", "decimal", minimum=0),
            _field("product_length_cm", "decimal", minimum=0),
            _field("product_height_cm", "decimal", minimum=0),
            _field("product_width_cm", "decimal", minimum=0),
        ),
        ("product_id",),
    ),
    "sellers": _table(
        "sellers",
        "olist_sellers_dataset.csv",
        (
            _field("seller_id", "id", required=True),
            _field("seller_zip_code_prefix", "zip"),
            _field("seller_city", "text"),
            _field("seller_state", "text"),
        ),
        ("seller_id",),
    ),
    "geolocation": _table(
        "geolocation",
        "olist_geolocation_dataset.csv",
        (
            _field("geolocation_zip_code_prefix", "zip"),
            _field("geolocation_lat", "coordinate", required=True),
            _field("geolocation_lng", "coordinate", required=True),
            _field("geolocation_city", "text"),
            _field("geolocation_state", "text"),
        ),
        ("source_row_id",),
    ),
    "category_translation": _table(
        "category_translation",
        "product_category_name_translation.csv",
        (
            _field("product_category_name", "id", required=True),
            _field("product_category_name_english", "text", required=True),
        ),
        ("product_category_name",),
    ),
}

TABLE_SPECS: Mapping[str, TableSpec] = MappingProxyType(_TABLE_SPECS)
SOURCE_FILES = frozenset(spec.source_file for spec in TABLE_SPECS.values())
MONEY_FIELDS = frozenset({"price", "freight_value", "payment_value"})
