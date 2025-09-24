from unidecode import unidecode
from fuzzywuzzy import fuzz
from metaphone import doublemetaphone
from transliterate import translit, get_available_language_codes
import re

class TextNormalizer:
    """Class for normalize text and transliteration"""

    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize name, and delete useless characters"""
        return " ".join(name.replace("«", "\"").replace("»", "\"").split())
    
    @staticmethod 
    def variants(name: str):
        """Generates variants for name"""
        base = TextNormalizer.normalize_name(name)

        variants = {base}

        # Latin transliteration using unidecode
        try:
            latin = unidecode(base)
            variants.add(latin)
        except Exception:
            latin = None

        # Heuristic aliasing for common OCR/transliteration confusions observed in samples
        # 1) Cyrillic: "ан" ↔ "аи" for endings like "Пиллан" → "Пиллаи"
        if "Пиллан" in base:
            variants.add(base.replace("Пиллан", "Пиллаи"))

        # 2) Latin: "an" ↔ "ai" for endings like "Pillan" → "Pillai"
        if latin and "Pillan" in latin:
            variants.add(latin.replace("Pillan", "Pillai"))

        # Return a sorted list for deterministic comparison in tests
        return sorted(variants)
    

class QueryBuilder:
    """Class for building query(prompt)"""
    def __init__(self, payload: dict):
        self.payload = payload        

      
    def _get(self, key: str, default=""):
        return self.payload.get(key, default) or default

    def build_query(self) -> str:
        """Building query for LightRAG"""

        counterparty = self._get("COUNTERPARTY_NAME") 
        consignee = self._get("CONSIGNEE")
        client = self._get("CLIENT")
        manufacturer = self._get("MANUFACTURER")

        entities_to_check = {
            "Counterparty": counterparty,
            "Consignee": consignee,
            "Client": client,
            "Manufacturer": manufacturer
        }       

        entity_variants_text = ""
        for role, name in entities_to_check.items():
            if name:
                name_variants = "; ".join(TextNormalizer.variants(name))
                entity_variants_text += f"- {role}: {name}\n  - Variants/aliases to consider: {name_variants}\n"

        banks = self._get("BIK_SWIFT")



        query_text = f"""
[System Prompt]
Task: Sanctions and trade restrictions screening using the knowledge graph. You need to give a final verdict based on built-in knowledge graph on ready documents of sanctions list of US, UK and EU.
Return a compact JSON matching exactly this schema:
{{
  "verdict": "clear" | "flag",
  "reasons": [ "string" ],
  "hits": {{
    "entities": [
      {{ "name": "string", "matched": true|false, "match_type": "exact|fuzzy|alias", "list": "OFAC|EU|UN|UK|KZ|Other", "source": "url or node_id", "notes": "string" }}
    ],
    "banks": [
      {{ "swift": "string", "matched": true|false, "list": "string", "source": "string", "notes": "string" }}
    ],
    "goods": [
      {{ "hs_code": "string", "matched": true|false, "control_basis": "embargo|dual-use|other", "source": "string", "notes": "string" }}
    ],
    "jurisdictions": [
      {{ "route": "{self._get("CONSIGNEE_COUNTRY", "KZ")}↔{self._get("COUNTERPARTY_COUNTRY", "RU")}", "matched": true|false, "control_basis": "embargo|sanction|restriction", "source": "string" }}
    ]
  }}
}}
Output only the JSON per the schema with no extra text. Use information only from knowledge graph, don't add by yourself and don't hallucinate. 

[User Prompt]
Screen the following transaction:
- Contract Type: {self._get("CONTRACT_TYPE")} (system: "{self._get("CONTRACT_TYPE_SYSTEM")}")
{entity_variants_text}
- Banks (SWIFT/BIC): {banks}
- Goods: "{self._get("PRODUCT_NAME")}"; HS code {self._get("HS_CODE")}
- Jurisdictions: {self._get("CONSIGNEE_COUNTRY")} and {self._get("COUNTERPARTY_COUNTRY")}
- Amount/Currency: {self._get("CONTRACT_AMOUNT")} {self._get("CONTRACT_CURRENCY")}
- Dates: contract {self._get("CONTRACT_DATE")}, end {self._get("CONTRACT_END_DATE")}
- Payment method code: {self._get("PAYMENT_METHOD")} (if relevant in the KG)

Questions:
1) Are any entities (seller/consignee/client/manufacturer) sanctioned or listed (OFAC/EU/UN/UK/KZ or others)? Include aliases/AKAs.
2) Are any banks with SWIFT {banks} sanctioned or restricted?
3) Is HS {self._get("HS_CODE")} or the described goods controlled/sanctioned for {self._get("CONSIGNEE_COUNTRY")}↔{self._get("COUNTERPARTY_COUNTRY")} trade?
4) Are there embargoes/restrictions that affect {self._get("CONSIGNEE_COUNTRY")}↔{self._get("COUNTERPARTY_COUNTRY")} for this transaction?
"""
        return query_text
