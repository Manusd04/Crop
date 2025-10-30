"""
Database schema for AgriClimate system
EXACT column names from DuckDB (all lowercase with underscores)
"""

SCHEMA = {
    "crop_production_raw": [
        "state", "district", "crop", "crop_year", "season", 
        "area", "production", "yield"
    ],
    
    "groundwater_raw": [
        "sr__no_", 
        "state_name_with_lgd_code", 
        "district_name_with_lgd_code",
        "block_name_with_lgd_code", 
        "gp_name_with_lgd_code", 
        "village",
        "site_name", 
        "type", 
        "source", 
        "well_id", 
        "latitude", 
        "longitude",
        "well_depth__meters_", 
        "aquifer",
        "pre_monsoon_2015__meters_below_ground_level_",
        "post_monsoon_2015__meters_below_ground_level_",
        "pre_monsoon_2016__meters_below_ground_level_",
        "post_monsoon_2016__meters_below_ground_level_",
        "pre_monsoon_2017__meters_below_ground_level_",
        "post_monsoon_2017__meters_below_ground_level_",
        "pre_monsoon_2018__meters_below_ground_level_",
        "post_monsoon_2018__meters_below_ground_level_",
        "pre_monsoon_2019__meters_below_ground_level_",
        "post_monsoon_2019__meters_below_ground_level_",
        "pre_monsoon_2020__meters_below_ground_level_",
        "post_monsoon_2020__meters_below_ground_level_",
        "pre_monsoon_2021__meters_below_ground_level_",
        "post_monsoon_2021__meters_below_ground_level_",
        "pre_monsoon_2022__meters_below_ground_level_",
        "post_monsoon_2022__meters_below_ground_level_"
    ],
    
    "rainfall_raw": [
        "s_no", 
        "district", 
        "day_actual_mm_",
        "day_normal_mm_",
        "day_%dep",
        "day_category", 
        "period_actual_mm_",
        "period_normal_mm_",
        "period_%dep", 
        "period_category"
    ],
    
    "market_price": [
        "state", 
        "district", 
        "market", 
        "commodity", 
        "variety", 
        "grade",
        "arrival_date", 
        "min_price", 
        "max_price", 
        "modal_price"
    ],
    
    "temperature": [
        "year", 
        "annual", 
        "unnamed:_2",
        "jan_feb", 
        "unnamed:_4",
        "mar_may", 
        "unnamed:_6",
        "jun_sep", 
        "unnamed:_8",
        "oct_dec", 
        "unnamed:_10"
    ]
}