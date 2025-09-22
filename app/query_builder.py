from unidecode import unidecode
from fuzzywuzzy import fuzz
from metaphone import doublemetaphone
from transliterate import translit, get_available_language_codes
import re

def normalize_name(name: str) -> str:
  return " ".join(name.replace("«","\"").replace("»","\"").split())

def to_latin(text: str) -> str:
  return unidecode(text) if text else ""

def transliterate_advanced(text: str) -> list:
  variants = [text]
  if re.search(r'[а-яё]', text, re.I):
    try:
      variants.append(translit(text, 'ru', reversed=True))
    except:
      pass
  if re.search(r'[\u0600-\u06FF]', text):
    try:
      variants.append(translit(text, 'ar', reversed=True))
    except:
      pass
  if re.search(r'[\u4e00-\u9fff]', text):
    try:
      variants.append(translit(text, 'zh', reversed=True))
    except:
      pass
  variants.append(to_latin(text))
  return list(set(variants))

def phonetic_variants(name: str) -> list:
  variants = []
  for part in name.split():
    primary, secondary = doublemetaphone(part)
    if primary:
      variants.append(primary)
    if secondary:
      variants.append(secondary)
  return variants

def fuzzy_variants(name: str, threshold=80) -> list:
  variants = [name]
  no_form = re.sub(r'\b(ООО|АО|ЗАО|ОАО|ТОО|LLC|INC)\b', '', name, flags=re.I).strip()
  if no_form != name:
    variants.append(no_form)
  return list(set(variants))

def variants(name: str) -> list:
  if not name:
    return []
  
  base = normalize_name(name)
  all_variants = set()
  
  translit_vars = transliterate_advanced(base)
  all_variants.update(translit_vars)
  
  phonetic_vars = phonetic_variants(base)[:2]
  all_variants.update(phonetic_vars)
  
  fuzzy_vars = fuzzy_variants(base)
  all_variants.update(fuzzy_vars)
  
  return [v for v in list(all_variants)[:5] if v]

def build_query(payload: dict) -> str:
  get = lambda k, default="": payload.get(k, default) or default
  
  counterparty = get("COUNTERPARTY_NAME")
  consignee = get("CONSIGNEE")
  client = get("CLIENT")
  manufacturer = get("MANUFACTURER")
  
  entities_to_check = {
    "Counterparty": counterparty,
    "Consignee": consignee,
    "Client": client,
    "Manufacturer": manufacturer
  }
  
  entity_variants_text = ""
  for role, name in entities_to_check.items():
    if name:
      name_variants = "; ".join(variants(name))
      entity_variants_text += f"- {role}: {to_latin(name)}\n  - Variants/aliases to consider: {name_variants}\n"

  banks = to_latin(get("BIK_SWIFT"))
  product_name = to_latin(get("PRODUCT_NAME"))
  contract_type = to_latin(get("CONTRACT_TYPE"))
  contract_type_system = to_latin(get("CONTRACT_TYPE_SYSTEM"))
  
  consignee_country = to_latin(get("CONSIGNEE_COUNTRY", "KZ"))
  counterparty_country = to_latin(get("COUNTERPARTY_COUNTRY", "RU"))
  
  query_text = f"""
Task: Sanctions and trade restrictions screening using the knowledge graph.
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
    {{ "route": "{consignee_country}↔{counterparty_country}", "matched": true|false, "control_basis": "embargo|sanction|restriction", "source": "string" }}
  ]
  }}
}}

Screen the following transaction:
- Contract Type: {contract_type} (system: "{contract_type_system}")
{entity_variants_text}
- Banks (SWIFT/BIC): {banks}
- Goods: "{product_name}"; HS code {get("HS_CODE")}
- Jurisdictions: {consignee_country} and {counterparty_country}
- Amount/Currency: {get("CONTRACT_AMOUNT")} {get("CONTRACT_CURRENCY")}
- Dates: contract {get("CONTRACT_DATE")}, end {get("CONTRACT_END_DATE")}
- Payment method code: {get("PAYMENT_METHOD")} (if relevant in the KG)

Questions:
1) Are any entities (seller/consignee/client/manufacturer) sanctioned or listed (OFAC/EU/UN/UK/KZ or others)? Include aliases/AKAs. **Return only the best matching sanctioned entity per role if found, or omit if no matches.**
2) Are any banks with SWIFT {banks} sanctioned or restricted? **Return only sanctioned banks if found.**
3) Is HS {get("HS_CODE")} or the described goods controlled/sanctioned for {consignee_country}↔{counterparty_country} trade? **Return only controlled goods if found.**
4) Are there embargoes/restrictions that affect {consignee_country}↔{counterparty_country} for this transaction? **Return only affected jurisdictions if found.**

Output only the JSON per the schema with no extra text. If no hits, return empty lists for entities/banks/goods/jurisdictions.
"""
  return query_text

