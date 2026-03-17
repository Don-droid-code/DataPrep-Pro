"""
synonyms.py — DataPrep Pro v2
Financial column name standardisation dictionary.
Maps common multilingual aliases → canonical English financial column names.
"""

CANONICAL_NAMES = [
    "Year","Revenue","COGS","Gross Profit","Operating Profit","EBITDA",
    "Interest Expense","Net Income","Total Assets","Current Assets",
    "Current Liabilities","Inventory","Accounts Receivable","Total Liabilities",
    "Total Equity","Long Term Debt","Net Debt","Cash","CapEx","Free Cash Flow",
    "ROE","ROA","ROCE","EPS","Dividend",
]

SYNONYMS: dict[str, list[str]] = {
    "Year": [
        "year","yr","fiscal year","fy","period","date","exercise",
        "année","exercice","periodo","jahr","geschäftsjahr",
        "financial year","reporting year","fiscal_year","year_end",
    ],
    "Revenue": [
        "revenue","revenues","sales","net sales","total sales","gross sales",
        "turnover","net revenue","total revenue","net turnover",
        "operating revenue","total turnover","sales revenue","rev","ca","tot rev",
        "chiffre d'affaires","chiffre daffaires","ventes","ventes nettes",
        "produits des ventes","ca net","recettes",
        "umsatz","umsatzerlöse","erlöse","gesamtumsatz",
        "ingresos","ventas","facturación","ingresos netos","ricavi","fatturato",
    ],
    "COGS": [
        "cogs","cost of goods sold","cost of sales","cost of revenue",
        "cost of products","direct costs","production costs","manufacturing costs",
        "cgs","cos",
        "coût des ventes","cout des ventes","coût des marchandises vendues",
        "coût de production","achats consommés",
        "warenaufwand","materialaufwand","herstellungskosten",
        "costo de ventas","costo de las ventas",
    ],
    "Gross Profit": [
        "gross profit","gross margin","gross income","gross earnings",
        "marge brute","résultat brut","bénéfice brut","profit brut",
        "rohertrag","bruttogewinn","margen bruto","beneficio bruto",
    ],
    "Operating Profit": [
        "operating profit","operating income","operating earnings",
        "ebit","income from operations","profit from operations",
        "operating result","earnings before interest and taxes",
        "résultat d'exploitation","résultat opérationnel","bénéfice d'exploitation",
        "betriebsergebnis","betriebsgewinn",
        "resultado de explotación","resultado operativo","baii",
    ],
    "EBITDA": [
        "ebitda","ebitda margin","earnings before interest taxes depreciation amortization",
        "operating ebitda","adjusted ebitda",
    ],
    "Interest Expense": [
        "interest expense","interest cost","finance costs","financial costs",
        "interest paid","interest charges","borrowing costs",
        "net interest expense","interest and similar charges",
        "charges financières","frais financiers","intérêts et charges assimilées",
        "zinsaufwand","finanzierungskosten",
        "gastos financieros","gastos por intereses",
    ],
    "Net Income": [
        "net income","net profit","net earnings","profit after tax",
        "net result","profit for the year","bottom line","pat",
        "net income after tax","net profit after tax","profit attributable",
        "résultat net","bénéfice net","profit net","résultat de l'exercice",
        "jahresüberschuss","nettogewinn","jahresergebnis",
        "resultado neto","beneficio neto","utilidad neta","utile netto",
    ],
    "Total Assets": [
        "total assets","assets total","total asset","sum of assets",
        "total actif","actif total","bilan total","total du bilan",
        "bilanzsumme","gesamtvermögen","summe der aktiva",
        "total activos","activos totales",
    ],
    "Current Assets": [
        "current assets","total current assets","short-term assets",
        "current asset","net current assets",
        "actif circulant","actifs courants","actif à court terme",
        "umlaufvermögen","activos corrientes","activo circulante",
    ],
    "Current Liabilities": [
        "current liabilities","total current liabilities","short-term liabilities",
        "current liability",
        "passif circulant","passifs courants","dettes à court terme",
        "kurzfristige verbindlichkeiten","pasivos corrientes","pasivo circulante",
    ],
    "Inventory": [
        "inventory","inventories","stock","stocks","merchandise",
        "raw materials","finished goods","goods for resale",
        "inventaire","stocks et en-cours","vorräte","lagerbestand",
        "inventarios","existencias",
    ],
    "Accounts Receivable": [
        "accounts receivable","trade receivables","debtors",
        "receivables","ar","trade debtors","net receivables",
        "créances clients","créances commerciales","clients",
        "forderungen aus lieferungen und leistungen","forderungen",
        "cuentas por cobrar","deudores comerciales",
    ],
    "Total Liabilities": [
        "total liabilities","liabilities total","total debt",
        "total obligations","sum of liabilities",
        "total dettes","total passif","passif total","dettes totales",
        "gesamtverbindlichkeiten","summe der passiva",
        "pasivos totales","total pasivos",
    ],
    "Total Equity": [
        "total equity","shareholders equity","stockholders equity",
        "net worth","book value","owners equity","total shareholders equity",
        "equity","shareholders funds",
        "capitaux propres","fonds propres","situation nette",
        "eigenkapital","nettovermögen","patrimonio neto","fondos propios",
    ],
    "Long Term Debt": [
        "long term debt","long-term debt","long term liabilities",
        "non-current liabilities","long term borrowings",
        "long term loans","non current debt",
        "dettes long terme","dettes à long terme","emprunts long terme",
        "langfristige verbindlichkeiten","langfristige schulden",
        "deuda a largo plazo","pasivos no corrientes",
    ],
    "Net Debt": [
        "net debt","dette nette","nettoverschuldung",
        "deuda neta","indebitamento netto",
    ],
    "Cash": [
        "cash","cash and equivalents","cash and cash equivalents",
        "trésorerie","liquidités","kassenbestand","efectivo","caja",
    ],
    "CapEx": [
        "capex","capital expenditure","capital expenditures",
        "property plant and equipment","ppe additions",
        "investissements","dépenses d'investissement",
        "investitionen","inversiones en capital","gastos de capital",
    ],
    "Free Cash Flow": [
        "free cash flow","fcf","free cashflow",
        "flux de trésorerie disponible","freier cashflow",
    ],
    "ROE": ["roe","return on equity","return on shareholders equity"],
    "ROA": ["roa","return on assets","return on total assets"],
    "ROCE": ["roce","return on capital employed"],
    "EPS": ["eps","earnings per share","bpa","bénéfice par action"],
    "Dividend": ["dividend","dividends","dividend per share","dps","dividende","dividendes"],
}
