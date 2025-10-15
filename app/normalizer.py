import re
from typing import List, Set
from unidecode import unidecode
import logging

try:
    import jellyfish
    JELLYFISH_AVAILABLE = True
except ImportError:
    JELLYFISH_AVAILABLE = False
    logging.warning("jellyfish not installed. Phonetic matching disabled.")

try:
    from cleanco import basename
    CLEANCO_AVAILABLE = True
except ImportError:
    CLEANCO_AVAILABLE = False
    logging.warning("cleanco not installed. Using fallback legal form removal.")

logger = logging.getLogger(__name__)


class AdvancedTextNormalizer:
    """Advanced text normalization for entity matching"""
    
    # Comprehensive list of legal entities from multiple countries
    LEGAL_FORMS = {
        # Russia/CIS
        'ПАО', 'АО', 'ООО', 'ЗАО', 'ОАО', 'ТОО', 'ИП', 'ТДО', 'НАО',
        # English
        'LLC', 'Ltd', 'Limited', 'Co', 'Corp', 'Corporation', 'Company',
        'Inc', 'Incorporated', 'PJSC', 'JSC', 'OJSC', 'PLC', 'LLP', 'LP',
        # Europe
        'S.A.', 'SA', 'GmbH', 'AG', 'SpA', 'SPA', 'BV', 'NV', 'Oy',
        'S.r.l', 'SRL', 'SARL', 'S.L.', 'SL',
        # Asia
        'Pte', 'Pty', 'Sdn', 'Bhd', 'K.K.', 'G.K.',
    }
    
    BANK_KEYWORDS = {
        'en': ['Bank', 'Banking', 'Financial', 'Finance', 'Credit', 'Savings'],
        'ru': ['Банк', 'Банка', 'Кредит', 'Финанс'],
        'misc': ['Branch', 'Филиал', 'Отделение', 'Head Office', 'HQ']
    }
    
    LOCATION_KEYWORDS = [
        'в г.', 'г.', 'город', 'city', 'Moscow', 'Moskva', 'St. Petersburg', 'SPb',
        'Astana', 'Almaty', 'Алматы', 'Астана', 'Москва', 'Санкт-Петербург'
    ]
    
    @staticmethod
    def clean_name(name: str) -> str:
        """Basic cleaning and normalization"""
        if not name or not isinstance(name, str):
            return ""
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Normalize quotes
        name = name.replace('«', '"').replace('»', '"')
        name = name.replace(''', "'").replace(''', "'")
        name = name.replace('`', "'")
        
        return name.strip()
    
    @staticmethod
    def remove_legal_forms(name: str) -> str:
        """Remove legal entity forms"""
        if CLEANCO_AVAILABLE:
            try:
                clean = basename(name)
                return clean if clean else name
            except Exception as e:
                logger.debug(f"cleanco failed for '{name}': {e}")
        
        # Fallback to manual removal
        result = name
        for form in AdvancedTextNormalizer.LEGAL_FORMS:
            result = re.sub(rf'\b{re.escape(form)}\b\.?', '', result, flags=re.IGNORECASE)
        return ' '.join(result.split()).strip()
    
    @staticmethod
    def extract_core_name(name: str) -> Set[str]:
        """Extract core business name using multiple strategies"""
        variants = set()
        
        # Strategy 1: Text in quotes
        quotes = re.findall(r'["\'"«]([^"\'»]+)["\'"»]', name)
        variants.update(q.strip() for q in quotes if len(q.strip()) > 2)
        
        # Strategy 2: After legal forms
        # "ООО Газпром" → "Газпром"
        for form in AdvancedTextNormalizer.LEGAL_FORMS:
            pattern = rf'\b{re.escape(form)}\b\.?\s+([A-ZА-ЯЁ][^\s,]+(?:\s+[A-ZА-ЯЁ][^\s,]+)*)'
            matches = re.findall(pattern, name, re.IGNORECASE)
            variants.update(m.strip() for m in matches if len(m.strip()) > 2)
        
        # Strategy 3: Before legal forms
        # "Газпром ООО" → "Газпром"
        for form in AdvancedTextNormalizer.LEGAL_FORMS:
            pattern = rf'([A-ZА-ЯЁ][^\s,]+(?:\s+[A-ZА-ЯЁ][^\s,]+)*)\s+\b{re.escape(form)}\b'
            matches = re.findall(pattern, name, re.IGNORECASE)
            variants.update(m.strip() for m in matches if len(m.strip()) > 2)
        
        return variants
    
    @staticmethod
    def transliteration_variants(name: str) -> Set[str]:
        """Generate transliteration variants"""
        variants = {name}
        
        # Unidecode (Cyrillic → Latin)
        try:
            latin = unidecode(name)
            if latin != name:
                variants.add(latin)
        except Exception as e:
            logger.debug(f"unidecode failed for '{name}': {e}")
        
        return variants
    
    @staticmethod
    def phonetic_variants(name: str) -> Set[str]:
        """Generate phonetic representations"""
        if not JELLYFISH_AVAILABLE:
            return set()
        
        variants = set()
        
        try:
            # Metaphone (good for English)
            mp = jellyfish.metaphone(name)
            if mp:
                variants.add(mp)
            
            # Soundex (classic algorithm)
            sx = jellyfish.soundex(name)
            if sx:
                variants.add(sx)
            
            # Match Rating Codex
            mrc = jellyfish.match_rating_codex(name)
            if mrc:
                variants.add(mrc)
        except Exception as e:
            logger.debug(f"phonetic encoding failed for '{name}': {e}")
        
        return variants
    
    @staticmethod
    def normalize_spacing(name: str) -> Set[str]:
        """Handle spacing variations"""
        variants = {name}
        
        # Remove all spaces (for acronyms)
        no_space = name.replace(' ', '')
        if len(no_space) > 3:
            variants.add(no_space)
        
        # CamelCase
        if ' ' in name:
            camel = ''.join(word.capitalize() for word in name.split())
            if len(camel) > 3:
                variants.add(camel)
        
        return variants
    
    @staticmethod
    def remove_location_info(name: str) -> str:
        """Remove location/city information"""
        result = name
        for loc in AdvancedTextNormalizer.LOCATION_KEYWORDS:
            result = re.sub(rf'\b{re.escape(loc)}\b', '', result, flags=re.IGNORECASE)
        
        # Remove parentheses with location
        result = re.sub(r'\([^)]*\)', '', result)
        
        return ' '.join(result.split()).strip()
    
    @classmethod
    def generate_all_variants(cls, name: str, entity_type: str = 'company') -> List[str]:
        """
        Generate comprehensive list of name variants
        
        Args:
            name: Original name
            entity_type: 'company', 'bank', or 'person'
        
        Returns:
            List of unique variants sorted by relevance
        """
        if not name or not isinstance(name, str):
            return []
        
        all_variants = set()
        
        # Step 1: Clean
        clean = cls.clean_name(name)
        all_variants.add(clean)
        
        # Step 2: Remove legal forms
        no_legal = cls.remove_legal_forms(clean)
        if no_legal:
            all_variants.add(no_legal)
        
        # Step 3: Remove location info
        no_location = cls.remove_location_info(clean)
        if no_location:
            all_variants.add(no_location)
        
        # Step 4: Extract core names
        core_names = cls.extract_core_name(clean)
        all_variants.update(core_names)
        
        # Step 5: Transliteration for all variants so far
        transliteration_base = set(all_variants)
        for variant in transliteration_base:
            all_variants.update(cls.transliteration_variants(variant))
        
        # Step 6: Handle spacing
        spacing_base = set(all_variants)
        for variant in spacing_base:
            all_variants.update(cls.normalize_spacing(variant))
        
        # Step 7: Bank-specific cleaning
        if entity_type == 'bank':
            bank_cleaned = set()
            for variant in all_variants:
                cleaned = variant
                for lang, keywords in cls.BANK_KEYWORDS.items():
                    for kw in keywords:
                        cleaned = re.sub(rf'\b{re.escape(kw)}\b', '', cleaned, flags=re.IGNORECASE)
                cleaned = ' '.join(cleaned.split()).strip()
                if cleaned and len(cleaned) > 2:
                    bank_cleaned.add(cleaned)
            all_variants.update(bank_cleaned)
        
        # Filter out invalid variants
        filtered = [
            v for v in all_variants 
            if v and len(v) > 2 and not v.isspace() and v not in cls.LEGAL_FORMS
        ]
        
        # Sort: longer names first (more specific), then alphabetically
        return sorted(set(filtered), key=lambda x: (-len(x), x.lower()))


class TextNormalizer:
    """
    Simple wrapper class for backward compatibility
    Provides easy-to-use interface matching original API
    """
    
    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize name and remove useless characters"""
        return AdvancedTextNormalizer.clean_name(name)
    
    @staticmethod
    def variants(name: str) -> List[str]:
        """Generate variants for company/entity names"""
        return AdvancedTextNormalizer.generate_all_variants(name, entity_type='company')
    
    @staticmethod
    def bank_variants(name: str) -> List[str]:
        """Generate variants specifically for bank names"""
        return AdvancedTextNormalizer.generate_all_variants(name, entity_type='bank')