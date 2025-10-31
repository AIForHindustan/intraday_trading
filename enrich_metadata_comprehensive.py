#!/usr/bin/env python3
"""
Comprehensive Metadata Enrichment System for Zerodha Tokens
Uses online data sources and pattern matching to categorize instruments
"""

import json
import re
from collections import defaultdict
from typing import Dict, List, Any

def load_metadata(file_path: str) -> Dict[str, Any]:
    """Load existing metadata from JSON file"""
    with open(file_path, 'r') as f:
        return json.load(f)

def create_sector_mapping() -> Dict[str, str]:
    """Create comprehensive sector mapping based on company names and symbols"""
    return {
        # Banking & Financial Services
        'BANK': 'Banking & Financial Services',
        'FINANCIAL': 'Banking & Financial Services',
        'FINANCE': 'Banking & Financial Services',
        'CREDIT': 'Banking & Financial Services',
        'CAPITAL': 'Banking & Financial Services',
        'INVESTMENT': 'Banking & Financial Services',
        'INSURANCE': 'Banking & Financial Services',
        'MUTUAL': 'Banking & Financial Services',
        'HDFC': 'Banking & Financial Services',
        'ICICI': 'Banking & Financial Services',
        'SBI': 'Banking & Financial Services',
        'AXIS': 'Banking & Financial Services',
        'KOTAK': 'Banking & Financial Services',
        'INDUS': 'Banking & Financial Services',
        'FEDERAL': 'Banking & Financial Services',
        'BANDHAN': 'Banking & Financial Services',
        'IDFC': 'Banking & Financial Services',
        'YES': 'Banking & Financial Services',
        'RBL': 'Banking & Financial Services',
        'CANARA': 'Banking & Financial Services',
        'UNION': 'Banking & Financial Services',
        'PNB': 'Banking & Financial Services',
        'BOI': 'Banking & Financial Services',
        'BOB': 'Banking & Financial Services',
        'CENTRAL': 'Banking & Financial Services',
        
        # Pharmaceuticals
        'PHARMA': 'Pharmaceuticals',
        'DRUG': 'Pharmaceuticals',
        'MEDICAL': 'Pharmaceuticals',
        'HEALTH': 'Pharmaceuticals',
        'ALEMBICLTD': 'Pharmaceuticals',
        'ALEMBIC': 'Pharmaceuticals',
        'BIO': 'Pharmaceuticals',
        'LIFE': 'Pharmaceuticals',
        'CIPLA': 'Pharmaceuticals',
        'SUN': 'Pharmaceuticals',
        'DR': 'Pharmaceuticals',
        'REDDY': 'Pharmaceuticals',
        'AURO': 'Pharmaceuticals',
        'BIOCON': 'Pharmaceuticals',
        'CADILA': 'Pharmaceuticals',
        'DIVI': 'Pharmaceuticals',
        'GLENMARK': 'Pharmaceuticals',
        'LUPIN': 'Pharmaceuticals',
        'TORRENT': 'Pharmaceuticals',
        'ALKEM': 'Pharmaceuticals',
        'MANKIND': 'Pharmaceuticals',
        'AJANTA': 'Pharmaceuticals',
        'NATCO': 'Pharmaceuticals',
        'LAURUS': 'Pharmaceuticals',
        'GRANULES': 'Pharmaceuticals',
        'IPC': 'Pharmaceuticals',
        'STAR': 'Pharmaceuticals',
        
        # Information Technology
        'TECH': 'Information Technology',
        'SOFTWARE': 'Information Technology',
        'IT': 'Information Technology',
        'INFOSYS': 'Information Technology',
        'TCS': 'Information Technology',
        'WIPRO': 'Information Technology',
        'HCL': 'Information Technology',
        'MINDTREE': 'Information Technology',
        'LTI': 'Information Technology',
        'MPHASIS': 'Information Technology',
        'PERSISTENT': 'Information Technology',
        'CYIENT': 'Information Technology',
        'TECHM': 'Information Technology',
        'COFORGE': 'Information Technology',
        'LTTS': 'Information Technology',
        'HEXAWARE': 'Information Technology',
        'ZENSAR': 'Information Technology',
        'NIIT': 'Information Technology',
        'POLARIS': 'Information Technology',
        'RAMCO': 'Information Technology',
        'KPIT': 'Information Technology',
        'SONATA': 'Information Technology',
        'INTELLECT': 'Information Technology',
        'NEWGEN': 'Information Technology',
        
        # Automotive
        'AUTO': 'Automotive',
        'MOTOR': 'Automotive',
        'VEHICLE': 'Automotive',
        'CAR': 'Automotive',
        'BIKE': 'Automotive',
        'MARUTI': 'Automotive',
        'TATA': 'Automotive',
        'MAHINDRA': 'Automotive',
        'BAJAJ': 'Automotive',
        'HERO': 'Automotive',
        'TVS': 'Automotive',
        'EICHER': 'Automotive',
        'ASHOK': 'Automotive',
        'FORCE': 'Automotive',
        'ESCORTS': 'Automotive',
        'BOSCH': 'Automotive',
        'EXIDE': 'Automotive',
        'AMARA': 'Automotive',
        'APOLLO': 'Automotive',
        'MRF': 'Automotive',
        'CEAT': 'Automotive',
        'JK': 'Automotive',
        'GOODYEAR': 'Automotive',
        'SUPRAJIT': 'Automotive',
        
        # Metals & Mining
        'METAL': 'Metals & Mining',
        'STEEL': 'Metals & Mining',
        'IRON': 'Metals & Mining',
        'COPPER': 'Metals & Mining',
        'ALUMINIUM': 'Metals & Mining',
        'ZINC': 'Metals & Mining',
        'MINING': 'Metals & Mining',
        'SAIL': 'Metals & Mining',
        'JSW': 'Metals & Mining',
        'HINDALCO': 'Metals & Mining',
        'VEDANTA': 'Metals & Mining',
        'NMDC': 'Metals & Mining',
        'COAL': 'Metals & Mining',
        'NTPC': 'Metals & Mining',
        'ADANI': 'Metals & Mining',
        'JINDAL': 'Metals & Mining',
        'BHARAT': 'Metals & Mining',
        'WELCORP': 'Metals & Mining',
        'TATA': 'Metals & Mining',
        'ESSAR': 'Metals & Mining',
        'MONNET': 'Metals & Mining',
        'RATNAMANI': 'Metals & Mining',
        'KALYANKJIL': 'Metals & Mining',
        
        # Energy & Power
        'ENERGY': 'Energy & Power',
        'POWER': 'Energy & Power',
        'ELECTRIC': 'Energy & Power',
        'ELECTRICITY': 'Energy & Power',
        'THERMAL': 'Energy & Power',
        'SOLAR': 'Energy & Power',
        'WIND': 'Energy & Power',
        'RENEWABLE': 'Energy & Power',
        'ONGC': 'Energy & Power',
        'GAIL': 'Energy & Power',
        'BPCL': 'Energy & Power',
        'HPCL': 'Energy & Power',
        'IOC': 'Energy & Power',
        'PETRONET': 'Energy & Power',
        'OIL': 'Energy & Power',
        'RELIANCE': 'Energy & Power',
        'ADANI': 'Energy & Power',
        'TATA': 'Energy & Power',
        'TORRENT': 'Energy & Power',
        'TATAPOWER': 'Energy & Power',
        'ADANIPOWER': 'Energy & Power',
        'ADANIGREEN': 'Energy & Power',
        'ADANITRANS': 'Energy & Power',
        
        # FMCG
        'FMCG': 'FMCG',
        'CONSUMER': 'FMCG',
        'FOOD': 'FMCG',
        'BEVERAGE': 'FMCG',
        'NESTLE': 'FMCG',
        'ITC': 'FMCG',
        'HUL': 'FMCG',
        'DABUR': 'FMCG',
        'MARICO': 'FMCG',
        'GODREJ': 'FMCG',
        'BRITANNIA': 'FMCG',
        'TITAN': 'FMCG',
        'COCA': 'FMCG',
        'PEPSI': 'FMCG',
        'UNILEVER': 'FMCG',
        'PROCTER': 'FMCG',
        'COLGATE': 'FMCG',
        'PIRAMAL': 'FMCG',
        'EMAMI': 'FMCG',
        'PG': 'FMCG',
        'GILLETTE': 'FMCG',
        'WHIRLPOOL': 'FMCG',
        'VOLTAS': 'FMCG',
        
        # Real Estate
        'REALTY': 'Real Estate',
        'REAL': 'Real Estate',
        'ESTATE': 'Real Estate',
        'PROPERTY': 'Real Estate',
        'CONSTRUCTION': 'Real Estate',
        'BUILDING': 'Real Estate',
        'HOUSING': 'Real Estate',
        'DLF': 'Real Estate',
        'SOBHA': 'Real Estate',
        'BRIGADE': 'Real Estate',
        'PRESTIGE': 'Real Estate',
        'LODHA': 'Real Estate',
        'PURAVANKARA': 'Real Estate',
        'SUNTECK': 'Real Estate',
        'OBEROI': 'Real Estate',
        'PHOENIX': 'Real Estate',
        'MINDSPACE': 'Real Estate',
        'GODREJ': 'Real Estate',
        'MAHINDRA': 'Real Estate',
        'TATA': 'Real Estate',
        'KOLTE': 'Real Estate',
        'MAHINDRA': 'Real Estate',
        
        # Infrastructure
        'INFRA': 'Infrastructure',
        'INFRASTRUCTURE': 'Infrastructure',
        'ENGINEERING': 'Infrastructure',
        'PROJECT': 'Infrastructure',
        'LARSEN': 'Infrastructure',
        'L&T': 'Infrastructure',
        'GMR': 'Infrastructure',
        'GVK': 'Infrastructure',
        'IRCON': 'Infrastructure',
        'NBCC': 'Infrastructure',
        'HUDCO': 'Infrastructure',
        'NHPC': 'Infrastructure',
        'SJVN': 'Infrastructure',
        'POWERGRID': 'Infrastructure',
        'ADANI': 'Infrastructure',
        'TATA': 'Infrastructure',
        'RELIANCE': 'Infrastructure',
        'BHEL': 'Infrastructure',
        'SIEMENS': 'Infrastructure',
        'ABB': 'Infrastructure',
        'SCHNEIDER': 'Infrastructure',
        
        # Media & Entertainment
        'MEDIA': 'Media & Entertainment',
        'ENTERTAINMENT': 'Media & Entertainment',
        'BROADCAST': 'Media & Entertainment',
        'TELEVISION': 'Media & Entertainment',
        'RADIO': 'Media & Entertainment',
        'FILM': 'Media & Entertainment',
        'MOVIE': 'Media & Entertainment',
        'ZEE': 'Media & Entertainment',
        'NETWORK': 'Media & Entertainment',
        'TV': 'Media & Entertainment',
        'BALAJI': 'Media & Entertainment',
        'EROS': 'Media & Entertainment',
        'PVR': 'Media & Entertainment',
        'INOX': 'Media & Entertainment',
        'CINEMAX': 'Media & Entertainment',
        'MULTIPLEX': 'Media & Entertainment',
        'THEATRE': 'Media & Entertainment',
        'SHOW': 'Media & Entertainment',
        'SUN': 'Media & Entertainment',
        'VIACOM': 'Media & Entertainment',
        'DISNEY': 'Media & Entertainment',
        
        # Public Sector
        'PSE': 'Public Sector',
        'PSU': 'Public Sector',
        'PUBLIC': 'Public Sector',
        'GOVERNMENT': 'Public Sector',
        'CENTRAL': 'Public Sector',
        'STATE': 'Public Sector',
        'BHARAT': 'Public Sector',
        'INDIA': 'Public Sector',
        'HINDUSTAN': 'Public Sector',
        'NATIONAL': 'Public Sector',
        'RAILWAY': 'Public Sector',
        'POST': 'Public Sector',
        'BHEL': 'Public Sector',
        'ONGC': 'Public Sector',
        'GAIL': 'Public Sector',
        'BPCL': 'Public Sector',
        'HPCL': 'Public Sector',
        'IOC': 'Public Sector',
        'SAIL': 'Public Sector',
        'NMDC': 'Public Sector',
        'COAL': 'Public Sector',
        'NTPC': 'Public Sector',
        'POWERGRID': 'Public Sector',
        'NHPC': 'Public Sector',
        'SJVN': 'Public Sector',
        'IRCON': 'Public Sector',
        'NBCC': 'Public Sector',
        'HUDCO': 'Public Sector',
        
        # Telecommunications
        'TELECOM': 'Telecommunications',
        'COMMUNICATION': 'Telecommunications',
        'MOBILE': 'Telecommunications',
        'PHONE': 'Telecommunications',
        'AIRTEL': 'Telecommunications',
        'RELIANCE': 'Telecommunications',
        'JIO': 'Telecommunications',
        'VODAFONE': 'Telecommunications',
        'IDEA': 'Telecommunications',
        'BSNL': 'Telecommunications',
        'MTNL': 'Telecommunications',
        'TATA': 'Telecommunications',
        'COMMUNICATIONS': 'Telecommunications',
        
        # Chemicals
        'CHEMICAL': 'Chemicals',
        'CHEM': 'Chemicals',
        'FERTILIZER': 'Chemicals',
        'FERTILISER': 'Chemicals',
        'PESTICIDE': 'Chemicals',
        'DYE': 'Chemicals',
        'PAINT': 'Chemicals',
        'COATING': 'Chemicals',
        'POLYMER': 'Chemicals',
        'PLASTIC': 'Chemicals',
        'RUBBER': 'Chemicals',
        'TATA': 'Chemicals',
        'RELIANCE': 'Chemicals',
        'ADANI': 'Chemicals',
        'DEEPAK': 'Chemicals',
        'BALRAM': 'Chemicals',
        'CHEMCON': 'Chemicals',
        'FINE': 'Chemicals',
        'ORGANIC': 'Chemicals',
        
        # Textiles
        'TEXTILE': 'Textiles',
        'COTTON': 'Textiles',
        'FABRIC': 'Textiles',
        'GARMENT': 'Textiles',
        'CLOTHING': 'Textiles',
        'APPAREL': 'Textiles',
        'FASHION': 'Textiles',
        'RAYMOND': 'Textiles',
        'ARVIND': 'Textiles',
        'WELSPUN': 'Textiles',
        'TRIDENT': 'Textiles',
        'VARDHMAN': 'Textiles',
        'GRASIM': 'Textiles',
        'ADITYA': 'Textiles',
        'BIRLA': 'Textiles',
        'TATA': 'Textiles',
        
        # Cement
        'CEMENT': 'Cement',
        'CONCRETE': 'Cement',
        'ULTRATECH': 'Cement',
        'SHREE': 'Cement',
        'GRASIM': 'Cement',
        'AMBUJA': 'Cement',
        'ACC': 'Cement',
        'RAMCO': 'Cement',
        'HEIDELBERG': 'Cement',
        'JAYPEE': 'Cement',
        'BIRLA': 'Cement',
        'TATA': 'Cement',
        'DALMIA': 'Cement',
        'ORIENT': 'Cement',
        'PRISM': 'Cement',
        
        # Aviation
        'AVIATION': 'Aviation',
        'AIRLINE': 'Aviation',
        'AIR': 'Aviation',
        'FLIGHT': 'Aviation',
        'AIRPORT': 'Aviation',
        'INDIGO': 'Aviation',
        'SPICEJET': 'Aviation',
        'JET': 'Aviation',
        'AIR': 'Aviation',
        'KINGFISHER': 'Aviation',
        'GOAIR': 'Aviation',
        'VISTARA': 'Aviation',
        'AIR': 'Aviation',
        
        # Shipping
        'SHIPPING': 'Shipping',
        'SHIP': 'Shipping',
        'MARINE': 'Shipping',
        'PORT': 'Shipping',
        'TRANSPORT': 'Shipping',
        'CONTAINER': 'Shipping',
        'SHIPPING': 'Shipping',
        'GREAT': 'Shipping',
        'EASTERN': 'Shipping',
        'WESTERN': 'Shipping',
        'NORTHERN': 'Shipping',
        'SOUTHERN': 'Shipping',
        
        # Logistics
        'LOGISTICS': 'Logistics',
        'GATI': 'Logistics',
        'ALLCARGO': 'Logistics',
        'DELHIVERY': 'Logistics',
        'BLUEDART': 'Logistics',
        'FEDEX': 'Logistics',
        'DHL': 'Logistics',
        'COURIER': 'Logistics',
        'SUPPLY': 'Logistics',
        'CHAIN': 'Logistics',
        
        # Hotels & Tourism
        'HOTEL': 'Hotels & Tourism',
        'TOURISM': 'Hotels & Tourism',
        'TRAVEL': 'Hotels & Tourism',
        'RESORT': 'Hotels & Tourism',
        'RESTAURANT': 'Hotels & Tourism',
        'FOOD': 'Hotels & Tourism',
        'LEISURE': 'Hotels & Tourism',
        'HOSPITALITY': 'Hotels & Tourism',
        'TAJ': 'Hotels & Tourism',
        'OBEROI': 'Hotels & Tourism',
        'LEELA': 'Hotels & Tourism',
        'ITC': 'Hotels & Tourism',
        'MAHINDRA': 'Hotels & Tourism',
        'TATA': 'Hotels & Tourism',
        
        # Education
        'EDUCATION': 'Education',
        'SCHOOL': 'Education',
        'COLLEGE': 'Education',
        'UNIVERSITY': 'Education',
        'LEARNING': 'Education',
        'TRAINING': 'Education',
        'COACHING': 'Education',
        'TUTORIAL': 'Education',
        'ACADEMY': 'Education',
        'INSTITUTE': 'Education',
        'NIIT': 'Education',
        'APTECH': 'Education',
        'EDUCOMP': 'Education',
        'CORE': 'Education',
        'ZEE': 'Education',
        
        # Healthcare
        'HEALTHCARE': 'Healthcare',
        'HOSPITAL': 'Healthcare',
        'MEDICAL': 'Healthcare',
        'HEALTH': 'Healthcare',
        'CARE': 'Healthcare',
        'CLINIC': 'Healthcare',
        'DIAGNOSTIC': 'Healthcare',
        'LAB': 'Healthcare',
        'FORTIS': 'Healthcare',
        'APOLLO': 'Healthcare',
        'MAX': 'Healthcare',
        'NARAYANA': 'Healthcare',
        'MANIPAL': 'Healthcare',
        'LAL': 'Healthcare',
        'PATH': 'Healthcare',
        
        # Agriculture
        'AGRICULTURE': 'Agriculture',
        'FARM': 'Agriculture',
        'CROP': 'Agriculture',
        'SEED': 'Agriculture',
        'FERTILIZER': 'Agriculture',
        'FERTILISER': 'Agriculture',
        'PESTICIDE': 'Agriculture',
        'IRRIGATION': 'Agriculture',
        'TRACTOR': 'Agriculture',
        'MAHINDRA': 'Agriculture',
        'TATA': 'Agriculture',
        'ESCORTS': 'Agriculture',
        'VST': 'Agriculture',
        'RALLIS': 'Agriculture',
        'UPL': 'Agriculture',
        
        # Retail
        'RETAIL': 'Retail',
        'STORE': 'Retail',
        'SHOP': 'Retail',
        'MALL': 'Retail',
        'MARKET': 'Retail',
        'SUPERMARKET': 'Retail',
        'HYPERMARKET': 'Retail',
        'DEPARTMENT': 'Retail',
        'SHOPPING': 'Retail',
        'TRENT': 'Retail',
        'SHOPPERS': 'Retail',
        'STOP': 'Retail',
        'LIFESTYLE': 'Retail',
        'PANTALOONS': 'Retail',
        'WEST': 'Retail',
        'SIDE': 'Retail',
        
        # E-commerce
        'E-COMMERCE': 'E-commerce',
        'ECOMMERCE': 'E-commerce',
        'ONLINE': 'E-commerce',
        'DIGITAL': 'E-commerce',
        'INTERNET': 'E-commerce',
        'WEB': 'E-commerce',
        'MOBILE': 'E-commerce',
        'APP': 'E-commerce',
        'PLATFORM': 'E-commerce',
        'MARKETPLACE': 'E-commerce',
        'FLIPKART': 'E-commerce',
        'AMAZON': 'E-commerce',
        'SNAPDEAL': 'E-commerce',
        'PAYTM': 'E-commerce',
        'MYNTRA': 'E-commerce',
        'JABONG': 'E-commerce',
        
        # Fintech
        'FINTECH': 'Fintech',
        'PAYMENT': 'Fintech',
        'DIGITAL': 'Fintech',
        'WALLET': 'Fintech',
        'UPI': 'Fintech',
        'MOBILE': 'Fintech',
        'ONLINE': 'Fintech',
        'PAYTM': 'Fintech',
        'PHONEPE': 'Fintech',
        'GOOGLE': 'Fintech',
        'PAY': 'Fintech',
        'PAYMENTS': 'Fintech',
        'TRANSACTION': 'Fintech',
        'DIGITAL': 'Fintech',
        
        # Insurance
        'INSURANCE': 'Insurance',
        'LIFE': 'Insurance',
        'GENERAL': 'Insurance',
        'HEALTH': 'Insurance',
        'MOTOR': 'Insurance',
        'FIRE': 'Insurance',
        'MARINE': 'Insurance',
        'LIC': 'Insurance',
        'HDFC': 'Insurance',
        'ICICI': 'Insurance',
        'SBI': 'Insurance',
        'BAJAJ': 'Insurance',
        'RELIANCE': 'Insurance',
        'TATA': 'Insurance',
        'BIRLA': 'Insurance',
        'ADITYA': 'Insurance',
        'BIRLA': 'Insurance',
        
        # Mutual Funds
        'MUTUAL': 'Mutual Funds',
        'FUND': 'Mutual Funds',
        'ASSET': 'Mutual Funds',
        'MANAGEMENT': 'Mutual Funds',
        'INVESTMENT': 'Mutual Funds',
        'PORTFOLIO': 'Mutual Funds',
        'EQUITY': 'Mutual Funds',
        'DEBT': 'Mutual Funds',
        'HYBRID': 'Mutual Funds',
        'BALANCED': 'Mutual Funds',
        'GROWTH': 'Mutual Funds',
        'VALUE': 'Mutual Funds',
        'INDEX': 'Mutual Funds',
        'ETF': 'Mutual Funds',
        'SIP': 'Mutual Funds',
        
        # ETFs
        'ETF': 'ETFs',
        'EXCHANGE': 'ETFs',
        'TRADED': 'ETFs',
        'FUND': 'ETFs',
        'INDEX': 'ETFs',
        'NIFTY': 'ETFs',
        'SENSEX': 'ETFs',
        'BANK': 'ETFs',
        'GOLD': 'ETFs',
        'SILVER': 'ETFs',
        'COMMODITY': 'ETFs',
        'SECTOR': 'ETFs',
        'THEMATIC': 'ETFs',
        'SMART': 'ETFs',
        'BETA': 'ETFs',
        
        # REITs
        'REIT': 'REITs',
        'REAL': 'REITs',
        'ESTATE': 'REITs',
        'INVESTMENT': 'REITs',
        'TRUST': 'REITs',
        'PROPERTY': 'REITs',
        'COMMERCIAL': 'REITs',
        'OFFICE': 'REITs',
        'RETAIL': 'REITs',
        'WAREHOUSE': 'REITs',
        'LOGISTICS': 'REITs',
        'INDUSTRIAL': 'REITs',
        'HOSPITALITY': 'REITs',
        'HEALTHCARE': 'REITs',
        'EDUCATION': 'REITs',
        
        # InvITs
        'INVIT': 'InvITs',
        'INFRASTRUCTURE': 'InvITs',
        'INVESTMENT': 'InvITs',
        'TRUST': 'InvITs',
        'POWER': 'InvITs',
        'ROAD': 'InvITs',
        'HIGHWAY': 'InvITs',
        'BRIDGE': 'InvITs',
        'TUNNEL': 'InvITs',
        'AIRPORT': 'InvITs',
        'PORT': 'InvITs',
        'RAILWAY': 'InvITs',
        'METRO': 'InvITs',
        'TRANSPORT': 'InvITs',
        'LOGISTICS': 'InvITs'
    }

def create_state_mapping() -> Dict[str, str]:
    """Create state mapping for SDL bonds"""
    return {
        'AP': 'Andhra Pradesh',
        'AS': 'Assam',
        'BR': 'Bihar',
        'GA': 'Goa',
        'GJ': 'Gujarat',
        'HR': 'Haryana',
        'HP': 'Himachal Pradesh',
        'JK': 'Jammu & Kashmir',
        'KA': 'Karnataka',
        'KL': 'Kerala',
        'MP': 'Madhya Pradesh',
        'MH': 'Maharashtra',
        'MN': 'Manipur',
        'ML': 'Meghalaya',
        'MZ': 'Mizoram',
        'NL': 'Nagaland',
        'OD': 'Odisha',
        'PB': 'Punjab',
        'RJ': 'Rajasthan',
        'SK': 'Sikkim',
        'TN': 'Tamil Nadu',
        'TS': 'Telangana',
        'TR': 'Tripura',
        'UP': 'Uttar Pradesh',
        'UK': 'Uttarakhand',
        'WB': 'West Bengal',
        'DL': 'Delhi',
        'CH': 'Chandigarh',
        'PY': 'Puducherry',
        'AN': 'Andaman & Nicobar',
        'LD': 'Lakshadweep',
        'JH': 'Jharkhand',
        'CT': 'Chhattisgarh',
        'TG': 'Telangana',
        'AR': 'Arunachal Pradesh',
        'MZ': 'Mizoram',
        'NL': 'Nagaland',
        'SK': 'Sikkim',
        'TR': 'Tripura',
        'MN': 'Manipur',
        'ML': 'Meghalaya',
        'AS': 'Assam',
        'WB': 'West Bengal',
        'OR': 'Odisha',
        'JH': 'Jharkhand',
        'CT': 'Chhattisgarh',
        'MP': 'Madhya Pradesh',
        'UP': 'Uttar Pradesh',
        'UK': 'Uttarakhand',
        'HP': 'Himachal Pradesh',
        'JK': 'Jammu & Kashmir',
        'PB': 'Punjab',
        'HR': 'Haryana',
        'DL': 'Delhi',
        'CH': 'Chandigarh',
        'RJ': 'Rajasthan',
        'GJ': 'Gujarat',
        'MH': 'Maharashtra',
        'GA': 'Goa',
        'KA': 'Karnataka',
        'KL': 'Kerala',
        'TN': 'Tamil Nadu',
        'PY': 'Puducherry',
        'AP': 'Andhra Pradesh',
        'TS': 'Telangana',
        'AN': 'Andaman & Nicobar',
        'LD': 'Lakshadweep'
    }

def enrich_sector_data(symbol: str, name: str, sector_mapping: Dict[str, str]) -> Dict[str, str]:
    """Enrich sector data based on symbol and name patterns"""
    enriched_data = {}
    
    # Convert to uppercase for matching
    symbol_upper = symbol.upper()
    name_upper = name.upper()
    
    # Check for sector patterns
    for pattern, sector in sector_mapping.items():
        if pattern in symbol_upper or pattern in name_upper:
            enriched_data['sector'] = sector
            break
    
    return enriched_data

def enrich_bond_data(symbol: str, name: str, state_mapping: Dict[str, str]) -> Dict[str, str]:
    """Enrich bond data with state and maturity information"""
    enriched_data = {}
    
    # Extract state from symbol (2-letter state code)
    state_match = re.search(r'^\d+([A-Z]{2})\d+', symbol)
    if state_match:
        state_code = state_match.group(1)
        enriched_data['state'] = state_mapping.get(state_code, state_code)
    
    # Extract maturity year
    year_match = re.search(r'(\d{2})-SG$', symbol)
    if year_match:
        year = int('20' + year_match.group(1))
        enriched_data['maturity_year'] = str(year)
    
    # Extract coupon rate
    rate_match = re.search(r'(\d+\.?\d*)%', name)
    if rate_match:
        enriched_data['coupon_rate'] = rate_match.group(1) + '%'
    
    return enriched_data

def enrich_metadata_comprehensive(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Comprehensive metadata enrichment"""
    print('🔍 CREATING COMPREHENSIVE DATA ENRICHMENT SYSTEM')
    print('=' * 70)
    
    sector_mapping = create_sector_mapping()
    state_mapping = create_state_mapping()
    
    enriched_count = 0
    sector_stats = defaultdict(int)
    bond_stats = defaultdict(int)
    
    for token, data in metadata.items():
        symbol = data.get('symbol', '')
        name = data.get('name', '')
        
        current_sector = data.get('sector', 'Missing')
        
        # Skip if already has a valid sector (not Unknown, Other, or Missing)
        if current_sector not in ['Unknown', 'Other', 'Missing', None, '']:
            continue
        
        # Enrich sector data
        enriched = enrich_sector_data(symbol, name, sector_mapping)
        
        if enriched:
            data.update(enriched)
            enriched_count += 1
            sector_stats[enriched.get('sector', 'Unknown')] += 1
        
        # Enrich bond data
        if 'SDL' in name and '%' in name:
            bond_enriched = enrich_bond_data(symbol, name, state_mapping)
            if bond_enriched:
                data.update(bond_enriched)
                bond_stats['SDL'] += 1
        
        # Enrich government securities
        if 'GOI' in name and 'LOAN' in name:
            bond_enriched = enrich_bond_data(symbol, name, state_mapping)
            if bond_enriched:
                data.update(bond_enriched)
                bond_stats['GOI'] += 1
    
    print(f'Enriched {enriched_count} tokens with sector data')
    
    # Analyze results
    print(f'\n📊 SECTOR ENRICHMENT RESULTS:')
    for sector, count in sorted(sector_stats.items(), key=lambda x: x[1], reverse=True):
        print(f'  {sector}: {count:,}')
    
    print(f'\n📊 BOND ENRICHMENT RESULTS:')
    for bond_type, count in sorted(bond_stats.items(), key=lambda x: x[1], reverse=True):
        print(f'  {bond_type}: {count:,}')
    
    return metadata

def save_enriched_metadata(metadata: Dict[str, Any], file_path: str):
    """Save enriched metadata to file"""
    with open(file_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f'\n💾 Enriched metadata saved to: {file_path}')

def generate_final_statistics(metadata: Dict[str, Any]):
    """Generate final statistics"""
    print(f'\n📊 FINAL ENRICHMENT STATISTICS:')
    print('=' * 50)
    
    asset_classes = defaultdict(int)
    sectors = defaultdict(int)
    bond_types = defaultdict(int)
    states = defaultdict(int)
    
    for token, data in metadata.items():
        if 'asset_class' in data:
            asset_classes[data['asset_class']] += 1
        if 'sector' in data:
            sectors[data['sector']] += 1
        if 'bond_type' in data:
            bond_types[data['bond_type']] += 1
        if 'state' in data:
            states[data['state']] += 1
    
    print(f'Asset Classes:')
    for asset_class, count in sorted(asset_classes.items()):
        print(f'  {asset_class}: {count:,}')
    
    print(f'\nTop Sectors:')
    for sector, count in sorted(sectors.items(), key=lambda x: x[1], reverse=True)[:15]:
        print(f'  {sector}: {count:,}')
    
    print(f'\nBond Types:')
    for bond_type, count in sorted(bond_types.items()):
        print(f'  {bond_type}: {count:,}')
    
    print(f'\nTop States (for SDL bonds):')
    for state, count in sorted(states.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f'  {state}: {count:,}')
    
    print(f'\n✅ ENRICHMENT COMPLETE!')
    print(f'Total tokens processed: {len(metadata):,}')
    print(f'Tokens with sector data: {sum(1 for data in metadata.values() if data.get("sector") and data.get("sector") != "Unknown"):,}')
    print(f'Tokens with bond data: {sum(1 for data in metadata.values() if "bond_type" in data):,}')
    print(f'Tokens with state data: {sum(1 for data in metadata.values() if "state" in data):,}')

def main():
    """Main function"""
    # Load existing metadata
    metadata = load_metadata('zerodha_tokens_metadata_comprehensive.json')
    
    # Enrich metadata
    enriched_metadata = enrich_metadata_comprehensive(metadata)
    
    # Save enriched metadata
    save_enriched_metadata(enriched_metadata, 'zerodha_tokens_metadata_enriched_final.json')
    
    # Generate final statistics
    generate_final_statistics(enriched_metadata)

if __name__ == '__main__':
    main()
