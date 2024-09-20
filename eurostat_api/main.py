from eurostat_api import EurostatAPI
import pandas as pd
import datetime

output_dir = "D:/Politecnico di Milano/Documentale DENG - PRIN-MIMO/Models/Data/raw databases/EUROSTAT_energy_emissions/raw_data_energy balances"

# basic url for browsing the full energy balance Eurostat dataset
base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_bal_c"

# query parameters
format = "JSON"
lang = "en"
unit = "TJ"

# time periods
time = [str(year) for year in range(1990, 2023)]

geo = [
    "EU27_2020", "BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR",
    "HR", "IT", "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT",
    "RO", "SI", "SK", "FI", "SE", "IS", "NO", "UK", "BA", "ME", "MD", "MK", 
    "GE", "AL", "RS", "TR", "UA", "XK", 
]

# energy balance items
nrg_bal = [
    "PPRD", "RCV_RCY", "IMP", "EXP", "STK_CHG", "GAE", "INTMARB", "GIC", "INTAVI", "NRGSUP",
    "GIC2020-2030", "PEC2020-2030", "FEC2020-2030", "TI_E", "TI_EHG_E", "TI_EHG_MAPE_E", "TI_EHG_MAPCHP_E", 
    "TI_EHG_MAPH_E", "TI_EHG_APE_E", "TI_EHG_APCHP_E", "TI_EHG_APH_E", "TI_EHG_EDHP", "TI_EHG_EB", 
    "TI_EHG_EPS", "TI_EHG_DHEP", "TI_EHG_CB", "TI_CO_E", "TI_BF_E", "TI_GW_E", "TI_RPI_E", 
    "TI_RPI_RI_E", "TI_RPI_BPI_E", "TI_RPI_PT_E", "TI_RPI_IT_E", "TI_RPI_DU_E", "TI_RPI_PII_E", 
    "TI_PF_E", "TI_BKBPB_E", "TI_CL_E", "TI_BNG_E", "TI_LBB_E", "TI_CPP_E", "TI_GTL_E", "TI_NSP_E", 
    "TO", "TO_EHG", "TO_EHG_MAPE", "TO_EHG_MAPCHP", "TO_EHG_MAPH", "TO_EHG_APE", "TO_EHG_APCHP", 
    "TO_EHG_APH", "TO_EHG_EDHP", "TO_EHG_EB", "TO_EHG_PH", "TO_EHG_OTH", "TO_CO", "TO_BF", "TO_GW", 
    "TO_RPI", "TO_RPI_RO", "TO_RPI_BKFLOW", "TO_RPI_PT", "TO_RPI_IT", "TO_RPI_PPR", "TO_RPI_PIR", 
    "TO_PF", "TO_BKBPB", "TO_CL", "TO_BNG", "TO_LBB", "TO_CPP", "TO_GTL", "TO_NSP", "NRG_E", 
    "NRG_EHG_E", "NRG_CM_E", "NRG_OIL_NG_E", "NRG_PF_E", "NRG_CO_E", "NRG_BKBPB_E", "NRG_GW_E", 
    "NRG_BF_E", "NRG_PR_E", "NRG_NI_E", "NRG_CL_E", "NRG_LNG_E", "NRG_BIOG_E", "NRG_GTL_E", "NRG_CPP_E", 
    "NRG_NSP_E", "DL", "AFC", "FC_NE", "TI_NRG_FC_IND_NE", "TI_NE", "NRG_NE", "FC_IND_NE", "FC_TRA_NE", 
    "FC_OTH_NE", "FC_E", "FC_IND_E", "FC_IND_IS_E", "FC_IND_CPC_E", "FC_IND_NFM_E", "FC_IND_NMM_E", 
    "FC_IND_TE_E", "FC_IND_MAC_E", "FC_IND_MQ_E", "FC_IND_FBT_E", "FC_IND_PPP_E", "FC_IND_WP_E", 
    "FC_IND_CON_E", "FC_IND_TL_E", "FC_IND_NSP_E", "FC_TRA_E", "FC_TRA_RAIL_E", "FC_TRA_ROAD_E", 
    "FC_TRA_DAVI_E", "FC_TRA_DNAVI_E", "FC_TRA_PIPE_E", "FC_TRA_NSP_E", "FC_OTH_E", "FC_OTH_CP_E", 
    "FC_OTH_HH_E", "FC_OTH_AF_E", "FC_OTH_FISH_E", "FC_OTH_NSP_E", "STATDIFF", "GEP", "GEP_MAPE", 
    "GEP_MAPCHP", "GEP_APE", "GEP_APCHP", "GHP", "GHP_MAPCHP", "GHP_MAPH", "GHP_APCHP", "GHP_APH",
]

# products categories
siec = [
    "TOTAL", "C0000X0350-0370", "C0110", "C0121", "C0129", "C0210", "C0220", "C0311", "C0312", 
    "C0320", "C0330", "C0340", "C0350-0370", "C0350", "C0360", "C0371", "C0379", "P1000", "P1100", 
    "P1200", "S2000", "G3000", "O4000XBIO", "O4100_TOT", "O4200", "O4300", "O4400X4410", "O4500", 
    "O4610", "O4620", "O4630", "O4640", "O4651", "O4652XR5210B", "O4653", "O4661XR5230B", "O4669", 
    "O4671XR5220B", "O4680", "O4691", "O4692", "O4693", "O4694", "O4695", "O4699", "RA000", "RA100", 
    "RA200", "RA300", "RA410", "RA420", "RA500", "RA600", "R5110-5150_W6000RI", "R5160", "R5210P", 
    "R5210B", "R5220P", "R5220B", "R5230P", "R5230B", "R5290", "R5300", "W6100", "W6210", "W6220", 
    "W6100_6220", "N900H", "E7000", "H8000", "BIOE", "FE"
]

# building and executing final query (chunked by geo)
query_params = {
    'format': format,
    'geo': geo,
    'time': time,
    'unit': unit,
    'nrg_bal': nrg_bal,
    'siec': siec,
    'lang': lang,
}

# scraping data for each country and returning the merged dataframe
# scraper = EurostatAPI(base_url, output_dir)

# scraper.scrape_data(
#     query_params=query_params,
#     filename="energy_balances",
#     file_format="txt",
#     chunk_query_by="geo",
# )


scraper = EurostatAPI(
    base_url="https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/meta?dataset=nrg_bal_c", 
    output_dir=output_dir
)

data = scraper.fetch_data_to_dataframe()
