import pytest
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app. import build_query, normalize_name, variants
from app.main import extract_json

def test_normalize_name():
    assert normalize_name('ОАО "Пиллан Точик"') == 'ОАО "Пиллан Точик"'
    assert normalize_name('  ТОО «ASHAN  Ой»  ') == 'ТОО "ASHAN Ой"'

def test_variants():
    name = 'ОАО "Пиллан Точик"'
    expected_variants = sorted(['OAO "Pillai Tochik"', 'OAO "Pillan Tochik"', 'ОАО "Пиллаи Точик"', 'ОАО "Пиллан Точик"'])
    assert variants(name) == expected_variants

def test_build_query():
    payload = {
      "BIK_SWIFT": "IRTYKZKA, HSBKKZKX, KZKOTJ22XXX",
      "CONTRACT_CURRENCY": "USD",
      "PAYMENT_CURRENCY": "USD",
      "CURRENCY_CONTRACT_NUMBER": "JSKT25-6-1",
      "CONTRACT_AMOUNT_TYPE": "общая",
      "CONSIGNOR": "ОАО \"Пиллан Точик\", Республика Таджикистан",
      "CONSIGNEE": "ТОО \"ASHAN Ой\", 100940012442, Республика Казахстан",
      "CONTRACT_DATE": "2025-06-09",
      "CONTRACT_END_DATE": "2025-12-31",
      "PRODUCT_CATEGORY": "0",
      "CLIENT": "ТОО \"ASHAN OIL\", БИН 100940012442, Казахстан",
      "CURRENCY_CONTRACT_TYPE_CODE": "1",
      "COUNTERPARTY_NAME": "ОАО \"Пиллаи Точик\", Республика Таджикистан",
      "PRODUCT_NAME": "Коконы урожая 2024-2025",
      "CROSS_BORDER": "1",
      "MANUFACTURER": "ОАО \"Пиллаи Точик\", Республика Таджикистан",
      "PAYMENT_METHOD": "14",
      "REPATRIATION_TERM": "180 дней",
      "DOCUMENT_REFERENCES": "Приложение №1",
      "COUNTERPARTY_COUNTRY": "TJ",
      "CONTRACT_AMOUNT": "780000,00",
      "HS_CODE": "5001000000",
      "CONTRACT_TYPE": "Импорт",
      "THIRD_PARTIES": None,
      "UN_CODE": "1",
      "CONTRACT_TYPE_SYSTEM": "02"
    }
    query = build_query(payload)
    assert "HS code 5001000000" in query
    assert 'ОАО "Пиллаи Точик"' in query
    assert "IRTYKZKA, HSBKKZKX, KZKOTJ22XXX" in query

def test_extract_json():
    json_str = '{"key": "value"}'
    assert extract_json(f"some text {json_str} some other text") == {"key": "value"}
    assert extract_json('no json here') is None
    assert extract_json('{"bad": json}') is None
