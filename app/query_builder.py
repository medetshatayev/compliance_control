import logging
import json
from .normalizer import TextNormalizer 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    """Class for transforming field array format to flat dictionary."""
    
    @staticmethod
    def transform_fields_to_payload(
        fields_data: dict,
        confidence_threshold: float = 0.8,
        ignore_confidence: bool = True
    ) -> dict:
        """
        Transform fields array format to flat payload dictionary.
        
        Args:
            fields_data: dict with 'fields'
            confidence_threshold: (оставлен для совместимости)
            ignore_confidence: если True — не фильтруем по confidence
        """
        if not isinstance(fields_data, dict) or 'fields' not in fields_data:
            logger.warning("Invalid input format. Expected dict with 'fields' key")
            return {}

        payload = {}
        
        for field in fields_data['fields']:
            if not isinstance(field, dict):
                continue

            if not ignore_confidence:
                conf_raw = field.get('confidence')
                if conf_raw is None:
                    conf = 1.0
                else:
                    try:
                        conf = float(conf_raw)
                    except Exception:
                        conf = 0.0
                if conf < confidence_threshold:
                    continue  # фильтруем только если ignore_confidence=False

            name_eng = field.get('name_eng', '')
            value = field.get('value', '')
            if not name_eng:
                continue
            
            if isinstance(value, list):
                payload[name_eng] = ', '.join(str(v) for v in value if v)
            elif isinstance(value, (str, int, float)):
                payload[name_eng] = str(value).strip()
            else:
                payload[name_eng] = "" if value is None else str(value)
        
        return payload
    


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
    
    @classmethod
    def from_flat_payload(cls, payload: dict):
        """Create QueryBuilder from flat key-value JSON.
        - Normalize into uppercase
        - Removes extra spaces
        """
        if not isinstance(payload, dict):
            return cls({})
        
        normalized: dict = {}
        for raw_key, raw_value in payload.items():
            if raw_key is None:
                continue
            key = str(raw_key).strip().upper().replace(" ", "_")
            value = raw_value
            if value is None:
                norm_value = ""
            elif isinstance(value, (list, tuple)):
                norm_value = ", ".join(str(v) for v in value if v is not None)
            else:
                norm_value = str(value)
            
            if key in normalized and normalized[key] and norm_value:
                if str(norm_value) not in str(normalized[key]):
                    normalized[key] = f"{normalized[key]}, {norm_value}"
            else:
                normalized[key] = norm_value
        
        counterparty_bank = normalized.get("COUNTERPARTY_BANK_NAME", "").strip()
        if counterparty_bank:
            if normalized.get("BIK_SWIFT"):
                existing = normalized["BIK_SWIFT"].strip()
                if counterparty_bank not in existing:
                    normalized["BIK_SWIFT"] = f"{existing}, {counterparty_bank}" if existing else counterparty_bank
            else:
                normalized["BIK_SWIFT"] = counterparty_bank
        
        correspondent_bank = normalized.get("CORRESPONDENT_BANK_NAME", "").strip()
        if correspondent_bank:
            if normalized.get("BIK_SWIFT"):
                existing = normalized["BIK_SWIFT"].strip()
                if correspondent_bank not in existing:
                    normalized["BIK_SWIFT"] = f"{existing}, {correspondent_bank}"
            else:
                normalized["BIK_SWIFT"] = correspondent_bank
        
        return cls(normalized)
    

    def build_query(self) -> str:
        """Building query for LightRAG"""
        
        cross_border = self._get("CROSS_BORDER", "0")
        route = self._get("ROUTE", "")
        counterparty_country = self._get("COUNTERPARTY_COUNTRY", "")
        
        full_data = self._collect_transaction_data()
        query_text = self._build_base_query()
        kz_as_transit = False
        if route and "-" in route:
            route_parts = [r.strip().upper() for r in route.split("-")]
            if len(route_parts) > 2 and "KZ" in route_parts[1:-1]:
                kz_as_transit = True
            elif len(route_parts) >= 2 and "KZ" in route_parts:
                kz_as_transit = True
        cross_border_inconsistency = False
        if cross_border == "0":
            if counterparty_country and counterparty_country.upper() not in ["KZ", ""]:
                cross_border_inconsistency = True
            if route and "-" in route:
                cross_border_inconsistency = True
        needs_goods_check = (
            cross_border == "1" or
            cross_border_inconsistency or
            kz_as_transit or
            (route and "-" in route) or
            (counterparty_country and counterparty_country.upper() not in ["KZ", ""])
        )
        if needs_goods_check:
            query_text += self._build_cross_border_instructions()
            if cross_border_inconsistency:
                query_text += """
                
                **CRITICAL WARNING: CROSS_BORDER Inconsistency Detected**
                - CROSS_BORDER field is "0" (domestic) 
                - BUT transaction involves international counterparty or multi-country route
                - This is a RED FLAG for potential sanctions circumvention scheme
                - MUST perform full goods screening and flag for enhanced review
                
                **Common circumvention patterns to check:**
                - CN → KZ → RU (using KZ to bypass China-Russia restrictions)
                - RU → KZ → Western countries (using KZ to re-export from Russia)
                - Any sanctioned country → KZ → destination (using KZ as laundering hub)
                """
        else:
            query_text += self._build_domestic_instructions()
        
        query_text += self._build_transaction_section(full_data)
        query_text += self._build_final_instructions("1" if needs_goods_check else "0")
        
        return query_text

    def _collect_transaction_data(self) -> dict:
        """Collect all transaction data fields with normalized variants"""
        return {
            "BIK_SWIFT": self._get("BIK_SWIFT"),
            "CONTRACT_CURRENCY": self._get("CONTRACT_CURRENCY"),
            "PAYMENT_CURRENCY": self._get("PAYMENT_CURRENCY"),
            "CURRENCY_CONTRACT_NUMBER": self._get("CURRENCY_CONTRACT_NUMBER"),
            "CONTRACT_AMOUNT_TYPE": self._get("CONTRACT_AMOUNT_TYPE"),
            "CONSIGNOR": self._get_entity_with_variants("CONSIGNOR", "company"),
            "CONSIGNEE": self._get_entity_with_variants("CONSIGNEE", "company"),
            "CONTRACT_DATE": self._get("CONTRACT_DATE"),
            "CONTRACT_END_DATE": self._get("CONTRACT_END_DATE"),
            "PRODUCT_CATEGORY": self._get("PRODUCT_CATEGORY"),
            "CLIENT": self._get_entity_with_variants("CLIENT", "company"),
            "CURRENCY_CONTRACT_TYPE_CODE": self._get("CURRENCY_CONTRACT_TYPE_CODE"),
            "COUNTERPARTY_NAME": self._get_entity_with_variants("COUNTERPARTY_NAME", "company"),
            "PRODUCT_NAME": self._get("PRODUCT_NAME"),
            "CONTRACT_DESCRIPTION": self._get("CONTRACT_DESCRIPTION"),
            "CROSS_BORDER": self._get("CROSS_BORDER", "0"),
            "MANUFACTURER": self._get_entity_with_variants("MANUFACTURER", "company"),
            "PAYMENT_METHOD": self._get("PAYMENT_METHOD"),
            "REPATRIATION_TERM": self._get("REPATRIATION_TERM"),
            "DOCUMENT_REFERENCES": self._get("DOCUMENT_REFERENCES"),
            "COUNTERPARTY_COUNTRY": self._get("COUNTERPARTY_COUNTRY"),
            "HS_CODE": self._get("HS_CODE"),
            "CONTRACT_TYPE": self._get("CONTRACT_TYPE"),
            "THIRD_PARTIES": self._get("THIRD_PARTIES"),
            "UN_CODE": self._get("UN_CODE"),
            "CONTRACT_TYPE_SYSTEM": self._get("CONTRACT_TYPE_SYSTEM"),
            "COUNTERPARTY_BANK_NAME": self._get("COUNTERPARTY_BANK_NAME"),
            "CORRESPONDENT_BANK_NAME": self._get("CORRESPONDENT_BANK_NAME"),
            "BANKS": self._get_banks_with_variants(),
            "ROUTE": self._get("ROUTE")
        }

    def _get_combined_banks(self) -> str:
        """Combine all bank fields into single BANKS field"""
        banks = []
        
        counterparty_bank = self._get("COUNTERPARTY_BANK_NAME")
        if counterparty_bank:
            banks.append(counterparty_bank)
        
        correspondent_bank = self._get("CORRESPONDENT_BANK_NAME")
        if correspondent_bank:
            banks.append(correspondent_bank)
        
        bank = self._get("BANK")
        if bank:
            banks.append(bank)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_banks = []
        for bank in banks:
            if bank not in seen:
                seen.add(bank)
                unique_banks.append(bank)
        
        return ", ".join(unique_banks) if unique_banks else ""

    def _get_banks_with_variants(self) -> str:
        """Get banks with normalized variants"""
        banks = self._get_combined_banks()
        if not banks:
            return ""
        
        # Generate variants for each bank
        all_variants = []
        for bank in banks.split(", "):
            bank_variants = TextNormalizer.bank_variants(bank.strip())
            all_variants.extend(bank_variants[:5])  # Limit to 5 variants per bank
        
        # Remove duplicates while preserving order
        seen = set()
        unique_variants = []
        for variant in all_variants:
            if variant not in seen:
                seen.add(variant)
                unique_variants.append(variant)
        
        return ", ".join(unique_variants)

    def _get_entity_with_variants(self, field_name: str, entity_type: str) -> str:
        """Get entity name with normalized variants"""
        name = self._get(field_name)
        if not name:
            return ""
        
        if entity_type == "bank":
            variants = TextNormalizer.bank_variants(name)
        else:
            variants = TextNormalizer.variants(name)
        
        # Combine original name with variants
        all_names = [name] + variants[:5]  # Limit to 5 variants
        
        # Remove duplicates while preserving order
        seen = set()
        unique_names = []
        for variant in all_names:
            if variant not in seen:
                seen.add(variant)
                unique_names.append(variant)
        
        return ", ".join(unique_names)

    def _build_base_query(self) -> str:
        return """
        ### Role and objective
        You are a sanctions compliance expert with access to up-to-date global sanctions lists from:
        - US OFAC
        - UK
        - EU

        Your task is to screen the transaction data and identify sanctions risks.

        ### Important Rules:
        1. If ANY entity, bank, or country matches a sanctions list (by name, SWIFT, BIK, or variant), verdict MUST be 'flag'.
        2. If no match found in ANY of the US/UK/EU lists, the verdict is 'clear'.
        3. Always check all variants, transliterations, and identifiers.
        4. Provide brief, factual explanations.
        5. Output ONLY the specified JSON structure - no additional text outside JSON.
        6. Matching policy:
           - Branch/filial names inherit parent bank/entity sanctions. If a name contains a known parent (e.g., ПАО "Совкомбанк"), treat it as that parent.
           - Do not require exact equality; base-name/transliteration match is sufficient.
           - If ANY sanctioned match is found in parties/banks, "check_parties.verdict" MUST be true. If goods section is present, set "goods.verdict" true as well (overall "verdict":"flag").

        ### CRITICAL RED FLAGS - MUST FLAG IF DETECTED:
        1. **CROSS_BORDER Inconsistency**: If CROSS_BORDER="0" (domestic) BUT:
           - COUNTERPARTY_COUNTRY is not KZ (e.g., RU, BY, CN, HK)
           - ROUTE contains multiple countries (e.g., "KZ-RU", "CN-KZ-RU")
           - CONTRACT_DESCRIPTION mentions "международн" or international operations
           - This indicates potential sanctions circumvention scheme

        2. **Sanctioned Jurisdictions**: Flag if COUNTERPARTY_COUNTRY or ROUTE includes:
           - RU (Russia), BY (Belarus), CN (China), HK (Hong Kong), IR (Iran), KP (North Korea)

        3. **Suspicious Routes**: Flag circumvention patterns:
           - CN-KZ-RU (China→Kazakhstan→Russia bypass)
           - RU-KZ-EU (Russia→Kazakhstan→EU re-export)
           - BY-KZ-RU (Belarus→Kazakhstan→Russia)
           - Any route where KZ is used as transit between sanctioned countries

        4. **Suspicious Banks**: Flag if bank names contain:
           - "Совкомбанк", "Сбербанк", "ВТБ", "Газпромбанк"
           - "Bank of China", "ICBC", "Agricultural Bank of China"

        5. **Transit Schemes**: Flag if ROUTE shows KZ as intermediary/transit country

        ### Processing Logic Based on CROSS_BORDER value:
        """

    def _build_cross_border_instructions(self) -> str:
        """Build instructions for cross-border transactions (CROSS_BORDER = 1)"""
        return """
        Return ONLY this JSON structure:
        {
          "verdict": "flag" | "clear",
          "checks": {
            "check_parties": { "verdict": true|false, "explanation": "..." },
            "goods": { "verdict": true|false, "explanation": "..." }
          }
        }

        Rules:
        - Scopes:
          - check_parties considers: COUNTERPARTY_NAME, CLIENT, CONSIGNEE, CONSIGNOR, MANUFACTURER, COUNTERPARTY_BANK_NAME, CORRESPONDENT_BANK_NAME, BIK_SWIFT, COUNTERPARTY_COUNTRY, PAYMENT_METHOD, CONTRACT_CURRENCY, PAYMENT_CURRENCY.
          - check_parties ignores: HS_CODE, PRODUCT_NAME, CONTRACT_DESCRIPTION, ROUTE, CONTRACT_TYPE, DOCUMENT_REFERENCES, AMOUNT.
          - goods considers: HS_CODE, PRODUCT_NAME, CONTRACT_DESCRIPTION, ROUTE, CONTRACT_TYPE, DOCUMENT_REFERENCES, PAYMENT_CURRENCY, COUNTERPARTY_COUNTRY.
          - goods ignores: CLIENT, CONSIGNEE, CONSIGNOR, MANUFACTURER, COUNTERPARTY_NAME, bank names/BIK_SWIFT, AMOUNT.
        
        - **RED FLAG CHECKS** (MUST flag if ANY detected):
          1. CROSS_BORDER="0" but COUNTERPARTY_COUNTRY≠KZ or ROUTE contains multiple countries
          2. COUNTERPARTY_COUNTRY in sanctioned jurisdictions (RU, BY, CN, HK, IR, KP)
          3. ROUTE shows circumvention patterns (CN-KZ-RU, RU-KZ-EU, etc.)
          4. Bank names contain sanctioned entities
          5. KZ used as transit country
        
        - Parties/banks: If ANY party or bank (by normalized base name or transliteration) matches US/UK/EU sanctions lists, set check_parties.verdict=true.
        - Branch logic: If a branch/filial name contains a parent bank/entity name (e.g., ПАО "Совкомбанк"), treat it as the parent for sanctions matching.
        - Goods check: Set goods.verdict=true only if HS_CODE indicates controlled items (93xx; sensitive 84xx/85xx), route indicates circumvention (e.g., CN-KZ-RU, RU-TR-EU), or energy 27xx to/from sanctioned jurisdictions.
        - Overall verdict is "flag" if either check_parties.verdict or goods.verdict is true; else "clear".
        - Write all explanations in Russian language.
        - Be concise in explanations.
        """

    def _build_domestic_instructions(self) -> str:
        """Build instructions for domestic transactions (CROSS_BORDER = 0)"""
        return """
        Return ONLY this JSON structure:
        {
          "verdict": "flag" | "clear",
          "checks": {
            "check_parties": { "verdict": true|false, "explanation": "..." }
          }
        }

        Rules:
        - **CRITICAL RED FLAG**: If CROSS_BORDER="0" BUT COUNTERPARTY_COUNTRY≠KZ or ROUTE contains multiple countries, this is a MAJOR inconsistency indicating potential sanctions circumvention. MUST flag.
        
        - If ANY party or bank (by normalized base name or transliteration) matches US/UK/EU sanctions lists, set check_parties.verdict=true and overall verdict="flag".
        - Branch logic: If a branch/filial name contains a parent bank/entity name, treat it as the parent for sanctions matching.
        - Otherwise verdict="clear".
        - Write all explanations in Russian language.
        - Be concise in explanations.
        """

    def _build_transaction_section(self, full_data: dict) -> str:
        return f"""
        
        ### Current Transaction to Screen:
        ```json
        {json.dumps({"data": full_data}, indent=2, ensure_ascii=False)}
        ```
        """

    def _build_final_instructions(self, cross_border: str) -> str:
        goods_instruction = ""
        if cross_border == "1":
            goods_instruction = "6. For goods verdict: true if goods/route/HS code are restricted, false if not"
        
        return f"""
        
        ### Final Instructions:
        1. Check each field systematically
        2. Consider transliterations and variants
        3. Return verdict "flag" if ANY sanctions found, "clear" if none found
        4. For check_parties verdict: true if ANY entity/bank is found in sanctions lists or suspected, false if NONE found
        {goods_instruction}
        
        Output ONLY valid JSON matching the examples above. No additional text.
        """