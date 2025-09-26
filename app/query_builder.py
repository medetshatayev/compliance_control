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


        query_text = f"""
        ### Role and objective
        You are a sanctions compliance expert with access to up-to-date global sanctions lists from sources like:
        - US (OFAC SDN List, Consolidated list, Terrorists OFAC, etc.)
        - UK (HM Treasury Consolidated List)
        - EU (EU Consolidated Financial Sanctions List

        Your task is to screen entities, organizations, countries, goods, banks, routes, contracts, manufacturers, HS codes for sanctions risks.
        Base verdicts ONLY on factual matches(e.g., exact or high-confidence fuzzy matches > 90% similarity). If no match, the verdict is False. Provide brief, factual explanations.
        
        # Reasoning strategy 
        Think step by step:
        1. Normalize and compare entity names/banks/HS codes/ against known sanctioned lists.
        2. Check for aliases, transliterations or variants.
        3. Evaluate routes for sanctioned countries(e.g., Russia, Iran)
        4. Assess goods/HS codes for export controls
        5. Cross-reference with contract type for any red flags.
        
        #Chain of thought
        You need to check each field of input JSON file separately. Then, you need to move to the next field and etc. Do one by one.
        
        Use only given sources, don't use other resources and 
        Output ONLY the specified JSON structure. No additional text, no references, no explanations outside the JSON. Don't add extra fields and don't change the structure. 
        """


        # Новый формат промпта
        # ...existing code...
        if cross_border == "0":
            query_text += f"""
         
        ### Context
        - Focus solely on US, UK, EU sanctions for entities and banks.
        - Sanctions context: US lists cover SDN(Specially Designated Nationals), Consolidated list; UK includes asset freeze; EU has consolidated lists for rinancial sanctions
        - Use fuzzy matching for names to account for variations, but require high confidence(> 90%)
        - No internet access needed; rely on known lists.
        
        ### Examples
        Example 1:
        Input:
        {
  "request_id": "req-790",
  "callback_url": null,
  "data": {
    "fields": [
      {
        "name": "Наименование клиента",
        "name_eng": "CLIENT", 
        "value": "ТОО \"Импортёр Казахстан\"",
        "confidence": 0.95
      },
      {
        "name": "Наименование контрагента",
        "name_eng": "COUNTERPARTY_NAME",
        "value": "ПАО \"Сбербанк России\"", 
        "confidence": 0.95
      },
      {
        "name": "Грузополучатель",
        "name_eng": "CONSIGNEE",
        "value": "Казахстанский получатель",
        "confidence": 0.95
      },
      {
        "name": "Производитель", 
        "name_eng": "MANUFACTURER",
        "value": "Российский поставщик",
        "confidence": 0.95
      },
      {
        "name": "БИК/SWIFT",
        "name_eng": "BIK_SWIFT", 
        "value": ["SABRRUMM"],
        "confidence": 0.95
      },
      {
        "name": "Пересечение РК",
        "name_eng": "CROSS_BORDER",
        "value": "0",
        "confidence": 0.95
      },
      {
        "name": "Маршрут",
        "name_eng": "ROUTE", 
        "value": "RU-KZ",
        "confidence": 0.95
      },
      {
        "name": "Код ТН ВЭД",
        "name_eng": "HS_CODE",
        "value": "8517709000", 
        "confidence": 0.95
      },
      {
        "name": "Наименование товара",
        "name_eng": "PRODUCT_NAME",
        "value": "Оборудование для телекоммуникаций",
        "confidence": 0.95
      },
      {
        "name": "Тип контракта",
        "name_eng": "CONTRACT_TYPE", 
        "value": "Импорт",
        "confidence": 0.95
      }
    ]
  }
}
 
        Process: Matches in all US/UK/EU lists.
        Output:
        {
  "verdict": "flag",
  "risk_level": "medium",
  "checks": {
    "check_parties": {
      "us": {
        "verdict": true,
        "explanation": "ПАО 'Сбербанк России'/Sberbank Rossii (including the SWIFT/BIC code SABRRUMM) is sanctioned by OFAC (US) and appears in the OFAC 24.09.2025 list. No information is available indicating that 'Казахстанский получатель', 'ТОО \"Импортёр Казахстан\"', or 'Российский поставщик' are sanctioned by the US; these names and their variants do not appear in the provided OFAC lists."
      },
      "uk": {
        "verdict": true,
        "explanation": "ПАО 'Сбербанк России'/Sberbank Rossii is sanctioned under the Russia (Sanctions) (EU Exit) Regulations 2019 and associated UK designations. No UK sanctions are found in the data for 'Казахстанский получатель', 'ТОО \"Импортёр Казахстан\"', or 'Российский поставщик'."
      },
      "eu": {
        "verdict": true,
        "explanation": "ПАО 'Сбербанк России'/Sberbank Rossii is subject to EU financial sanctions, including under various EU lists (e.g., Annex I to Regulation (EU) No 269/2014 and subsequent restrictive measures against Russian financial institutions). No evidence is found that 'Казахстанский получатель', 'ТОО \"Импортёр Казахстан\"', or 'Российский поставщик' are sanctioned by the EU."
      }
    }
  },
}
        ### Current task
        Screen these entities and banks:
        {entity_variants_text}
        - Banks (SWIFT/BIC): {banks}
        
        """
        else:
            query_text += f"""
        
        ### Context
        - Screen entitites, banks, goods (via HS code and description), route, and contract type.
        - Sanctions context: US lists cover SDN(Specially Designated Nationals), Consolidated list; UK includes asset freeze; EU has consolidated lists for rinancial sanctions
        - Goods restrictions: HS codes for military(e.g., 93xx), dual-use/
        - Contract types like "export" or "import" may trigger additional scrutiny if to restricted destinations.
        
        
        ### Examples
        Example 1:
        Input:
        {
  "request_id": "req-790",
  "callback_url": null,
  "data": {
    "fields": [
      {
        "name": "Наименование клиента",
        "name_eng": "CLIENT", 
        "value": "ТОО \"Импортёр Казахстан\"",
        "confidence": 0.95
      },
      {
        "name": "Наименование контрагента",
        "name_eng": "COUNTERPARTY_NAME",
        "value": "ПАО \"Сбербанк России\"", 
        "confidence": 0.95
      },
      {
        "name": "Грузополучатель",
        "name_eng": "CONSIGNEE",
        "value": "Казахстанский получатель",
        "confidence": 0.95
      },
      {
        "name": "Производитель", 
        "name_eng": "MANUFACTURER",
        "value": "Российский поставщик",
        "confidence": 0.95
      },
      {
        "name": "БИК/SWIFT",
        "name_eng": "BIK_SWIFT", 
        "value": ["SABRRUMM"],
        "confidence": 0.95
      },
      {
        "name": "Пересечение РК",
        "name_eng": "CROSS_BORDER",
        "value": "0",
        "confidence": 0.95
      },
      {
        "name": "Маршрут",
        "name_eng": "ROUTE", 
        "value": "RU-KZ",
        "confidence": 0.95
      },
      {
        "name": "Код ТН ВЭД",
        "name_eng": "HS_CODE",
        "value": "8517709000", 
        "confidence": 0.95
      },
      {
        "name": "Наименование товара",
        "name_eng": "PRODUCT_NAME",
        "value": "Оборудование для телекоммуникаций",
        "confidence": 0.95
      },
      {
        "name": "Тип контракта",
        "name_eng": "CONTRACT_TYPE", 
        "value": "Импорт",
        "confidence": 0.95
      }
    ]
  }
}
 
        Process: Matches in all US/UK/EU lists.
        Output:
        {
  "verdict": "flag",
  "risk_level": "medium",
  "checks": {
    "check_parties": {
      "us": {
        "verdict": true,
        "explanation": "ПАО 'Сбербанк России' (Sberbank Rossii) is listed by OFAC under multiple aliases and appears in OFAC sectoral sanctions records. No information links the Kazakh, client, or manufacturer entities to US restrictions."
      },
      "uk": {
        "verdict": true,
        "explanation": "ПАО 'Сбербанк России' (Sberbank Rossii) is a notable Russian bank subject to extensive UK Russia-related sanctions according to the Russia (Sanctions) (EU Exit) Regulations 2019. There is no evidence in the data of sanctions on the Kazakh or client entities."
      },
      "eu": {
        "verdict": true,
        "explanation": "ПАО 'Сбербанк России' appears as a key sanctioned entity for EU restrictive measures involving Russian banks; no evidence of EU sanctions on specified Kazakh parties or the manufacturer."
      }
    },
    "route": "RU-KZ",
    "contract_type": "Импорт",
    "goods": {
      "us": {
        "verdict": false,
        "explanation": "No explicit US sectoral or dual-use export controls or prohibitions were identified on telecommunications equipment under HS 8517709000 for export from Russia to Kazakhstan in the provided source data.",
        "hs code": "8517709000"
      },
      "uk": {
        "verdict": false,
        "explanation": "There are no explicit UK prohibitions found in the provided data on telecommunications equipment with HS 8517709000 for import into Kazakhstan from Russia.",
        "hs code": "8517709000"
      },
      "eu": {
        "verdict": false,
        "explanation": "The provided data does not indicate EU restrictions or prohibitions covering telecommunications equipment with HS 8517709000 for this route.",
        "hs code": "8517709000"
      }
    }
  },
}
        

Screen this transaction against all sanctions lists:
{entity_variants_text}
- Banks: {banks}
- Goods: {product_name}, HS: {hs_code}
- Route: {route}
- Contract Type: {contract_type}
"""
        return query_text
