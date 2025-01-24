from sqlalchemy import desc, and_, or_
from sqlalchemy.orm import Session, aliased
from datetime import datetime as dt, timedelta
from Services.db_connection import FFFilterTick
import pytz
import pandas as pd

def get_last_n_ticks(db: Session, condition:dict):
    filter_criteria = condition
    india_timezone = pytz.timezone("Asia/Kolkata")
    utc_timezone = pytz.utc

    india_now = dt.now(india_timezone)

    end_of_day = india_now

    # Convert start and end of day to UTC
    end_of_day_utc = end_of_day.astimezone(utc_timezone)
    
    end_of_day_utc_str = end_of_day_utc.strftime("%Y-%m-%d %H:%M:%S")
    
    query = db.query(FFFilterTick)
    filters = []
        
    if filter_criteria.get("timeRangeFrom", None) is not None:
        filters.append(FFFilterTick.timestamp >= filter_criteria["timeRangeFrom"])
        
    if filter_criteria.get("timeRangeTo", None) is not None:
        filters.append(FFFilterTick.timestamp <= filter_criteria["timeRangeTo"])
        
    if filter_criteria.get("timeRangeTo", None) == None:
        filters.append(FFFilterTick.timestamp <= end_of_day_utc_str)

    if filter_criteria.get("buildUp", None) is not None:
        buildup = (filter_criteria["buildUp"]).split("+")
        unique_data = list(dict.fromkeys(buildup))
        filters.append(FFFilterTick.oi_build_up.in_(unique_data))
    
        # Add conditions only if the filter_criteria has valid values
    if filter_criteria.get("side", None) is not None:
        filters.append(FFFilterTick.tag == filter_criteria["side"])

    if filter_criteria.get("sweep", None):
        value = f"""{filter_criteria["sweep"]}Sweep"""
        filters.append(or_(
                FFFilterTick.sweep1 == value,
                FFFilterTick.sweep2 == value,
                FFFilterTick.sweep3 == value
            ))

    if filter_criteria.get("powerSweep", None) is not None:
        value = f"""{filter_criteria["powerSweep"]}Sweep"""
        filters.append(FFFilterTick.power_sweep == value)

    if filter_criteria.get("block", None) is not None:
        value = f"""{filter_criteria.get("block", None)}"""
        filters.append(or_(FFFilterTick.block1 == "Block", FFFilterTick.block2 == "Block"))
        filters.append(FFFilterTick.tag == value)
    
    if filter_criteria.get("powerBlock", None):
        filters.append(FFFilterTick.power_block == "Block")

    if filter_criteria.get("optionType", None):
        value = filter_criteria["optionType"]
        if value == "Calls":
            filters.append(FFFilterTick.option_type == "CE")
        else:
            filters.append(FFFilterTick.option_type == "PE")

    # if filter_criteria.get("optionUnderlier", None):
    #     value = filter_criteria["optionUnderlier"]
    #     if value == "Stock":
    #         filters.append(FFFilterTick.option_type == "EQ")
    #     else:
    #         filters.append(FFFilterTick.option_type == "IN")

    if filter_criteria.get("volumeFrom", None) is not None:
        filters.append(FFFilterTick.volume >= filter_criteria["volumeFrom"])

    if filter_criteria.get("volumeTo", None) is not None:
        filters.append(FFFilterTick.volume <= filter_criteria["volumeTo"])

    if filter_criteria.get("oiFrom", None) is not None:
        filters.append(FFFilterTick.oi >= filter_criteria["oiFrom"])

    if filter_criteria.get("oiTo", None) is not None:
        filters.append(FFFilterTick.oi <= filter_criteria["oiTo"])
        
    if filter_criteria.get("oiChangeFrom", None) is not None:
        filters.append(FFFilterTick.oi_change >= filter_criteria["oiChangeFrom"])
    
    if filter_criteria.get("oiChangeTo", None) is not None:
        filters.append(FFFilterTick.oi_change <= filter_criteria["oiChangeTo"])
                                                                 
    # if filter_criteria.get("itmFrom", None) is not None:
    #     filters.append(FFFilterTick.strike_difference >= filter_criteria["itmFrom"])

    # if filter_criteria.get("itmTo", None) is not None:
    #     filters.append(FFFilterTick.strike_difference <= filter_criteria["itmTo"])
    
    query = query.filter(
        and_(*filters)
        ).order_by(FFFilterTick.id.desc()).limit(200)
    
    result = query.all()
    return result
    
def get_condition_rows(db:Session, from_date:dt, to_date: dt):
    query = db.query(FFFilterTick).filter(
        FFFilterTick.timestamp >= from_date,
        FFFilterTick.timestamp < to_date
    ).order_by(desc(FFFilterTick.id))
    
    return query.all()

# def get_last_n_ticks(db: Session, condition:dict):
#     filter_criteria=condition
#     filters = []
#     timeRange = []
#     query = db.query(FFFilterTick)
#     india_timezone = pytz.timezone("Asia/Kolkata")
#     india_now = dt.now(india_timezone)
#     start_of_day = dt(india_now.year, india_now.month, india_now.day)  # Start of today at 00:00:00
#     end_of_day = start_of_day + timedelta(days=1) - timedelta(seconds=1)
    
#     # Add conditions only if the filter_criteria has valid values
#     if filter_criteria.get("side", None) is not None:
#         filters.append(FFFilterTick.tag == filter_criteria["side"])

#     if filter_criteria.get("sweep", None):
#         filters.append(FFFilterTick.aggressor == filter_criteria["sweep"])

#     if filter_criteria.get("powerSweep", None):
#         filters.append(FFFilterTick.power_sweep == filter_criteria["powerSweep"])

#     if filter_criteria.get("block", None):
#         filters.append(FFFilterTick.block1 == filter_criteria["block"])

#     if filter_criteria.get("powerBlock", None):
#         filters.append(FFFilterTick.power_block == filter_criteria["powerBlock"])

#     if filter_criteria.get("optionType", None):
#         filters.append(FFFilterTick.option_type == filter_criteria["optionType"])

#     if filter_criteria.get("optionUnderlier", None):
#         filters.append(FFFilterTick.underlier_symbol == filter_criteria["optionUnderlier"])

#     if filter_criteria.get("volumeFrom", None) is not None:
#         filters.append(FFFilterTick.volume >= filter_criteria["volumeFrom"])

#     if filter_criteria.get("volumeTo", None) is not None:
#         filters.append(FFFilterTick.volume <= filter_criteria["volumeTo"])

#     if filter_criteria.get("oiFrom", None) is not None:
#         filters.append(FFFilterTick.oi >= filter_criteria["oiFrom"])

#     if filter_criteria.get("oiTo", None) is not None:
#         filters.append(FFFilterTick.oi <= filter_criteria["oiTo"])

#     if filter_criteria.get("itmFrom", None) is not None:
#         filters.append(FFFilterTick.strike_difference >= filter_criteria["itmFrom"])

#     if filter_criteria.get("itmTo", None) is not None:
#         filters.append(FFFilterTick.strike_difference <= filter_criteria["itmTo"])
        
#     if filter_criteria.get("timeRangeFrom", None) is not None:
#         timeRange.append(FFFilterTick.timestamp >= filter_criteria["timeRangeFrom"])
        
#     if filter_criteria.get("timeRangeFrom", None) == None:
#         timeRange.append(FFFilterTick.timestamp >= start_of_day)
        
#     if filter_criteria.get("timeRangeTo", None) is not None:
#         timeRange.append(FFFilterTick.timestamp >= filter_criteria["timeRangeTo"])
        
#     if filter_criteria.get("timeRangeTo", None) == None:
#         timeRange.append(FFFilterTick.timestamp <= end_of_day)
#     # Apply all the filters to the query
    
#     query = query.filter(and_(*timeRange))

#     # Create subquery
#     subquery = (query.order_by(desc(FFFilterTick.id)).subquery())
#     alias = aliased(FFFilterTick, subquery)

#     # Query results from alias
#     results = db.query(alias).order_by(alias.id.asc()).all()
#     return results
    
# def get_condition_rows(db:Session, from_date:dt, to_date: dt):
#     query = db.query(FFFilterTick).filter(
#         FFFilterTick.timestamp >= from_date,
#         FFFilterTick.timestamp < to_date
#     ).order_by(desc(FFFilterTick.id))
    
#     return query.all()