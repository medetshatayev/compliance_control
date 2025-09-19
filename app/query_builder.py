def normalize_name(name: str) -> str:
    return " ".join(name.replace("«","\"").replace("»","\"").split())

def variants(name: str):
    base = normalize_name(name)
    return [base]

def build_query(payload: dict) -> str:
    # extract fields defensively
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
            entity_variants_text += f"- {role}: {name}\n  - Variants/aliases to consider: {name_variants}\n"

    banks = get("BIK_SWIFT")
    
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
      {{ "route": "{get("CONSIGNEE_COUNTRY", "KZ")}↔{get("COUNTERPARTY_COUNTRY", "RU")}", "matched": true|false, "control_basis": "embargo|sanction|restriction", "source": "string" }}
    ]
  }}
}}

Screen the following transaction:
- Contract Type: {get("CONTRACT_TYPE")} (system: "{get("CONTRACT_TYPE_SYSTEM")}")
{entity_variants_text}
- Banks (SWIFT/BIC): {banks}
- Goods: "{get("PRODUCT_NAME")}"; HS code {get("HS_CODE")}
- Jurisdictions: {get("CONSIGNEE_COUNTRY")} and {get("COUNTERPARTY_COUNTRY")}
- Amount/Currency: {get("CONTRACT_AMOUNT")} {get("CONTRACT_CURRENCY")}
- Dates: contract {get("CONTRACT_DATE")}, end {get("CONTRACT_END_DATE")}
- Payment method code: {get("PAYMENT_METHOD")} (if relevant in the KG)

Questions:
1) Are any entities (seller/consignee/client/manufacturer) sanctioned or listed (OFAC/EU/UN/UK/KZ or others)? Include aliases/AKAs.
2) Are any banks with SWIFT {banks} sanctioned or restricted?
3) Is HS {get("HS_CODE")} or the described goods controlled/sanctioned for {get("CONSIGNEE_COUNTRY")}↔{get("COUNTERPARTY_COUNTRY")} trade?
4) Are there embargoes/restrictions that affect {get("CONSIGNEE_COUNTRY")}↔{get("COUNTERPARTY_COUNTRY")} for this transaction?

Output only the JSON per the schema with no extra text.
"""
    return query_text
