from unidecode import unidecode
from fuzzywuzzy import fuzz
from metaphone import doublemetaphone
from transliterate import translit, get_available_language_codes
import re
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    """Class for transforming field array format to flat dictionary"""
    
    @staticmethod
    def transform_fields_to_payload(fields_data: dict, confidence_threshold: float = 0.8) -> dict:
        """
        Transform fields array format to flat payload dictionary
        
        Args:
            fields_data: Dictionary containing 'fields' array
            
        Returns:
            Flat dictionary with field names as keys
        """
        if not isinstance(fields_data, dict) or 'fields' not in fields_data:
            logger.warning("Invalid input format. Expected dict with 'fields' key")
            return {}

  
        payload = {}
        
        for field in fields_data['fields']:
            if not isinstance(field, dict):
                continue

            conf_raw = field.get('confidence')
            if conf_raw is None:
                conf = 1.0
            else:
                try:
                    conf = float(conf_raw)
                except Exception:
                    conf = 0.0

            if conf < confidence_threshold:
                continue      

            name_eng = field.get('name_eng', '')
            value = field.get('value', '')
            
            if not name_eng:
                continue
            
            # Handle different value types
            if isinstance(value, list):
                # Join list values with comma and space
                payload[name_eng] = ', '.join(str(v) for v in value if v)
            elif isinstance(value, (str, int, float)):
                payload[name_eng] = str(value).strip()
            else:
                # Convert other types to string
                payload[name_eng] = str(value)
        
        return payload
    

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
        return sorted(variants)
    

class QueryBuilder:
    """Class for building query(prompt)"""
    def __init__(self, payload: dict):
        self.payload = payload        

      
    def _get(self, key: str, default=""):
        return self.payload.get(key, default) or default
    
    @classmethod
    def from_fields_data(cls, fields_data: dict):
        """
        Create QueryBuilder from fields array format
        
        Args:
            fields_data: Dictionary containing 'fields' array
            
        Returns:
            QueryBuilder instance
        """
        payload = DataTransformer.transform_fields_to_payload(fields_data)
        return cls(payload)
    
    def build_query(self) -> str:
        """Building query for LightRAG"""

        cross_border = self._get("CROSS_BORDER", "0")
        counterparty = self._get("COUNTERPARTY_NAME")
        consignee = self._get("CONSIGNEE")
        client = self._get("CLIENT")
        manufacturer = self._get("MANUFACTURER")
        banks = self._get("BIK_SWIFT")
        route = self._get("ROUTE")
        contract_type = self._get("CONTRACT_TYPE")
        hs_code = self._get("HS_CODE")
        product_name = self._get("PRODUCT_NAME")


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




        # Новый формат промпта
        # ...existing code...
        if cross_border == "0":
            query_text = f"""
Return only this JSON format with actual screening results:

{{
  "proverka_storon": {{
    "us": {{"verdict": true/false, "explanation": "actual result"}},
    "uk": {{"verdict": true/false, "explanation": "actual result"}},
    "eu": {{"verdict": true/false, "explanation": "actual result"}}
  }}
}}

Screen these entities and banks against all sanctions lists:
{entity_variants_text}
- Banks (SWIFT/BIC): {banks}

Check if any entities or banks are sanctioned by US, UK, or EU.
"""
        else:
            query_text = f"""
Return only this JSON format with actual screening results:

{{
  "proverka_storon": {{
    "us": {{"verdict": true/false, "explanation": "actual result"}},
    "uk": {{"verdict": true/false, "explanation": "actual result"}},
    "eu": {{"verdict": true/false, "explanation": "actual result"}}
  }},
  "route": "{route}",
  "contract_type": "{contract_type}",
  "goods": {{
    "us": {{"verdict": true/false, "explanation": "actual result", "hs code": "{hs_code}"}},
    "uk": {{"verdict": true/false, "explanation": "actual result", "hs code": "{hs_code}"}},
    "eu": {{"verdict": true/false, "explanation": "actual result", "hs code": "{hs_code}"}}
  }}
}}

Screen this transaction against all sanctions lists:
{entity_variants_text}
- Banks: {banks}
- Goods: {product_name}, HS: {hs_code}
- Route: {route}

Check entities, banks, goods, and route for any sanctions/restrictions.
"""
        return query_text
# ...existing code...