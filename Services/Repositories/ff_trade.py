from sqlalchemy import desc, and_, or_, case
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
    
    query = db.query(FFFilterTick,
            case(
                    [(FFFilterTick.symbol_type == "CE", ((FFFilterTick.strike - FFFilterTick.ltp) / FFFilterTick.ltp) * 100)],
                    [(FFFilterTick.symbol_type == "PE", ((FFFilterTick.ltp - FFFilterTick.strike) / FFFilterTick.ltp) * 100)],
                    else_=None
                ).label("otm_percent")
        )
    filters = []
           
    if filter_criteria.get("timeRangeFrom", None) is not None:
        filters.append(FFFilterTick.timestamp >= filter_criteria["timeRangeFrom"])
        
    if filter_criteria.get("timeRangeTo", None) is not None:
        filters.append(FFFilterTick.timestamp <= filter_criteria["timeRangeTo"])
        
    if filter_criteria.get("timeRangeTo", None) == None:
        filters.append(FFFilterTick.timestamp <= end_of_day_utc_str)
    # product
    if filter_criteria.get("products", None):
        series_filters = {
            "stocks": {"CE", "PE", "XX"},
            "options": {"CE", "PE"},
            "futures": {"XX"},
        }

        applicable_series = set()
        selected_products = filter_criteria.get("products", "").lower().split("+")
        
        if "stocks" in selected_products:
            applicable_series.update(series_filters["stocks"])
            if "options" in selected_products:
                applicable_series.difference_update(series_filters["options"])
            if "futures" in selected_products:
                applicable_series.difference_update(series_filters["futures"])
            filters.append(~FFFilterTick.symbol_type.in_(applicable_series))
        else:
            if "options" in selected_products:
                applicable_series.update(series_filters["options"])
            if "futures" in selected_products:
                applicable_series.update(series_filters["futures"])
            filters.append(FFFilterTick.symbol_type.in_(applicable_series))
    # OptionType
    # if filter_criteria.get("optionType", None):
    # Moneyness
    if filter_criteria.get("moneyness", None):
        moneyness = filter_criteria["moneyness"].split("+")
        filters.append(FFFilterTick.moneyness.in_(moneyness))
    # DTE
    # if filter_criteria.get("dteFrom", None):
    #     pass
    # if filter_criteria.get("dteTo", None):
    #     pass
    #  LTP
    if filter_criteria.get("ltpFrom", None) is not None:
        filters.append(FFFilterTick.ltp >= filter_criteria["ltpFrom"])
    
    if filter_criteria.get("ltpTo", None) is not None:
        filters.append(FFFilterTick.ltp <= filter_criteria["ltpTo"])
    #  LTQ
    if filter_criteria.get("ltqFrom", None) is not None:
        filters.append(FFFilterTick.last_size >= filter_criteria["ltpFrom"])
    
    if filter_criteria.get("ltqTo", None) is not None:
        filters.append(FFFilterTick.last_size <= filter_criteria["ltpTo"])
    # side
    if filter_criteria.get("side", None) is not None:
        filters.append(FFFilterTick.tag == filter_criteria["side"])

    # flags
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
    #  volume
    if filter_criteria.get("volumeFrom", None) is not None:
        filters.append(FFFilterTick.volume >= filter_criteria["volumeFrom"])

    if filter_criteria.get("volumeTo", None) is not None:
        filters.append(FFFilterTick.volume <= filter_criteria["volumeTo"])
    # Trade Value
    # OI
    if filter_criteria.get("oiFrom", None) is not None:
        filters.append(FFFilterTick.oi >= filter_criteria["oiFrom"])

    if filter_criteria.get("oiTo", None) is not None:
        filters.append(FFFilterTick.oi <= filter_criteria["oiTo"])
    # OI_Change
    # if filter_criteria.get("oiFrom", None) is not None:
    #     filters.append(FFFilterTick.oi >= filter_criteria["oiFrom"])

    # if filter_criteria.get("oiTo", None) is not None:
    #     filters.append(FFFilterTick.oi <= filter_criteria["oiTo"])
    # OI percent 
    # if filter_criteria.get("oiChangeFrom", None) is not None:
    #     filters.append(FFFilterTick.oi_change >= filter_criteria["oiChangeFrom"])
    
    # if filter_criteria.get("oiChangeTo", None) is not None:
    #     filters.append(FFFilterTick.oi_change <= filter_criteria["oiChangeTo"])
    
    #  Volume/OI
    if filter_criteria.get("ltqFrom", None) is not None:
        filters.append(FFFilterTick.last_size >= filter_criteria["ltpFrom"])
    
    if filter_criteria.get("ltqTo", None) is not None:
        filters.append(FFFilterTick.last_size <= filter_criteria["ltpTo"])
    #  ATP/vWAP
    if filter_criteria.get("ltqFrom", None) is not None:
        filters.append(FFFilterTick.last_size >= filter_criteria["ltpFrom"])
    
    if filter_criteria.get("ltqTo", None) is not None:
        filters.append(FFFilterTick.last_size <= filter_criteria["ltpTo"])
    #  OTM %
    # if filter_criteria.get("otm", None) is not None:
    #     if  filter_criteria["otm"] == ""
    #         filters.append(FFFilterTick.last_size >= filter_criteria["ltpFrom"])
    #     else:
    #         filters.append(FFFilterTick.)
    
    if filter_criteria.get("ltqTo", None) is not None:
        filters.append(FFFilterTick.last_size <= filter_criteria["ltpTo"])                                 
    # Cumulative
    # VolumeDelta
    # VolumeDeltaValue
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