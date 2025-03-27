from sqlalchemy import desc, and_, or_, case, func, Numeric, cast, Date
from sqlalchemy.orm import Session, aliased
from datetime import datetime as dt, timedelta
from Services.db_connection import FFFilterTick
import pytz
import pandas as pd
from dependencies import SECTOR_SYMBOLS, UNIQUE_INDUSTRIES, PREDEFINED_WATCHLIST, INDEX_FILENAME, SECTOR_SYMBOLS_DF

def get_last_n_ticks(db: Session, condition:dict):
    filter_criteria = condition
    
    series_filters = {
        "stocks": {"Equity"},
        "options": {"Call", "Put"},
        "futures": {"Futures"},
    }
    PRODUCT_CASE =  case(
            (FFFilterTick.symbol_type == 'CE', 'Call'),
            (FFFilterTick.symbol_type == 'PE', 'Put'),
            (FFFilterTick.symbol_type == 'XX', 'Futures'),
            else_="Equity"
        ).label('products')
    DTE_CASE = case(
        (~PRODUCT_CASE.in_(series_filters["stocks"]), func.date(FFFilterTick.expiry) - func.current_date()),
        else_=-1
    ).label("dte")
    FLOWTYPE_CASE = case(
        (and_(PRODUCT_CASE == "Equity", FFFilterTick.tag == "Buy"), "Bullish"),
        (and_(PRODUCT_CASE == "Call", FFFilterTick.tag == "Buy"), "Bullish"),
        (and_(PRODUCT_CASE == "Futures", FFFilterTick.tag == "Buy"), "Bullish"),
        (and_(PRODUCT_CASE == "Put", FFFilterTick.tag == "Sell"), "Bullish"),
        (and_(PRODUCT_CASE == "Equity", FFFilterTick.tag == "Sell"), "Bearish"),
        (and_(PRODUCT_CASE == "Put", FFFilterTick.tag == "Buy"), "Bearish"),
        (and_(PRODUCT_CASE == "Futures", FFFilterTick.tag == "Sell"), "Bearish"),
        (and_(PRODUCT_CASE == "Call", FFFilterTick.tag == "Sell"), "Bearish"),
        else_=""
    ).label("flowtype")
    OI_CAHNGE_PRECENT_CASE = case(
            (FFFilterTick.oi != 0, (FFFilterTick.oi_change/FFFilterTick.oi * 100)),
            else_=0
        ).label("oi_change_percent")
    VOL_OI_CASE =case(
            (FFFilterTick.oi != 0, FFFilterTick.volume / FFFilterTick.oi),
            else_=0
        ).label("vol_oi")
    OTM_PERCENT_CASE = case(
            (FFFilterTick.underlier_price != 0, cast((FFFilterTick.underlier_price - FFFilterTick.strike) * 100 / FFFilterTick.underlier_price, Numeric(10, 2))),
            else_=0
        ).label('otm_percent')
    query = db.query(
        FFFilterTick.id,
        FFFilterTick.symbol_id,
        FFFilterTick.symbol,
        FFFilterTick.exchange_token,
        FFFilterTick.timestamp,
        PRODUCT_CASE,
        FFFilterTick.moneyness,
        FFFilterTick.strike,
        FFFilterTick.expiry,
        DTE_CASE,
        FFFilterTick.ltp,
        FFFilterTick.last_size.label("ltq"),
        FFFilterTick.tag.label("side"),
        FLOWTYPE_CASE,
        FFFilterTick.volume,
        FFFilterTick.trade_value.label("trd_val"),
        FFFilterTick.oi,
        FFFilterTick.oi_change,
        OI_CAHNGE_PRECENT_CASE,
        VOL_OI_CASE,
        FFFilterTick.underlier_symbol,
        FFFilterTick.sweep1,
        FFFilterTick.sweep2,
        FFFilterTick.sweep3,
        FFFilterTick.power_sweep,
        FFFilterTick.block1,
        FFFilterTick.block2,
        FFFilterTick.power_block,
        OTM_PERCENT_CASE,
        FFFilterTick.delta_volume.label("vol_delta"),
        FFFilterTick.delta_volume_value.label("vol_delta_value"),
        FFFilterTick.delta_volume.label("cumulative_delta")
    )
    filters = []
           
    if filter_criteria.get("timeRangeFrom", None) is not None:
        timeRangeFrom = dt.strptime(filter_criteria["timeRangeFrom"], "%Y-%m-%dT%H%M%S")
        filters.append(FFFilterTick.timestamp >= timeRangeFrom)
    if filter_criteria.get("idRangeFrom", None):
        value = filter_criteria["idRangeFrom"]
        filters.append(FFFilterTick.id > value)
    if filter_criteria.get("timeRangeEnd", None) is not None:
        timeRnageEnd = dt.strptime(filter_criteria["timeRangeEnd"], "%Y-%m-%dT%H%M%S")
        filters.append(FFFilterTick.timestamp <= timeRnageEnd)
    if filter_criteria.get("idRangeEnd", None):
        value = filter_criteria["idRangeEnd"]
        filters.append(FFFilterTick.id < value)
    
    filter_symbols = []
    # Symbols
    if filter_criteria.get('symbols', None):
        symbols = filter_criteria.get("symbols").split("+")
        for symbol in symbols:
            filter_symbols.append(symbol)
    # Pre-defined Watchlist
    if filter_criteria.get("preWatchlist", None):
        pre_watchlists = []
        no_pre_watchlist = filter_criteria.get("preWatchlist").split("+")
        for index in no_pre_watchlist:
            pre_watchlists.append(INDEX_FILENAME[int(index)])
        for pre_watchlist in pre_watchlists:
            for item in PREDEFINED_WATCHLIST[pre_watchlist]:
                filter_symbols.append(item["Symbol"])
    #  Sectors
    if filter_criteria.get("sectors", None):
        industries = []
        no_industries = filter_criteria.get("sectors").split("+")
        for index in no_industries:
            industries.append(UNIQUE_INDUSTRIES[int(index)])
        for industry in industries:
            for item in SECTOR_SYMBOLS:
                if item["Industry"] == industry:
                    filter_symbols.append(item["Symbol"])
    if len(filter_symbols) > 0:
        filters.append(FFFilterTick.symbol.in_(filter_symbols))
    # product
    if filter_criteria.get("products", None):

        applicable_series = set()
        selected_products = filter_criteria.get("products").lower().split("+")
        for values in selected_products:
            applicable_series.update(series_filters[values])
        filters.append(PRODUCT_CASE.in_(applicable_series))
    # OptionType
    if filter_criteria.get("optionType", None):
        print(filter_criteria['optionType'])
        filters.append(PRODUCT_CASE == filter_criteria['optionType'])
    # Moneyness
    if filter_criteria.get("moneyness", None):
        moneynesses = filter_criteria.get("moneyness").split("+")
        filters.append(FFFilterTick.moneyness.in_(moneynesses))
    # DTE
    if filter_criteria.get("dteFrom", None):
        filters.append(and_(DTE_CASE > filter_criteria["dteFrom"], DTE_CASE < filter_criteria["dteTo"]))
    # LTP
    if filter_criteria.get("ltpFrom", None):
        filters.append(and_(FFFilterTick.ltp > filter_criteria["ltpFrom"], FFFilterTick.ltp < filter_criteria["ltpTo"]))
    # LTQ
    if filter_criteria.get("ltqFrom", None):
        filters.append(and_(FFFilterTick.last_size > filter_criteria["ltqFrom"], FFFilterTick.last_size < filter_criteria["ltqTo"]))
    
    # side
    if filter_criteria.get("side", None):
        filters.append(FFFilterTick.tag == filter_criteria["side"])
    #  flowType
    if filter_criteria.get("flowType", None):
        filters.append(FLOWTYPE_CASE == filter_criteria["flowType"])
    # flags
    if filter_criteria.get("sweep", None):
        sweeps = filter_criteria["sweep"].split("+")
        value = []
        for item in sweeps:
            value.append(f"{item}Sweep")
        filters.append(or_(
                FFFilterTick.sweep1.in_(value),
                FFFilterTick.sweep2.in_(value),
                FFFilterTick.sweep3.in_(value)
            ))

    if filter_criteria.get("powerSweep", None) is not None:
        powerSweeps = filter_criteria["powerSweep"].split("+")
        value = []
        for item in powerSweeps:
            value.append(f"{item}Sweep")
        filters.append(FFFilterTick.power_sweep.in_(value))
        
    if filter_criteria.get("block", None) or filter_criteria.get("powerBlock", None):
        if filter_criteria.get("block", None) is not None:
            blocks = filter_criteria["block"].split("+")
            value = []
            for item in blocks:
                value.append(f"{item}")
            filters.append(or_(FFFilterTick.block1 == "Block", FFFilterTick.block2 == "Block"))
        
        if filter_criteria.get("powerBlock", None):
            blocks = filter_criteria["block"].split("+")
            value = []
            for item in blocks:
                value.append(f"{item}")
            filters.append(FFFilterTick.power_block == "Block")
            
        filters.append(FFFilterTick.tag.in_(value))

    #  volume
    if filter_criteria.get("volumeFrom", None):
        value = filter_criteria["volumeFrom"]
        operator = filter_criteria["volumeOp"].lower()
        if operator == "more":
            filters.append(FFFilterTick.volume > value)
        elif operator == "less":
            filters.append(FFFilterTick.volume < value)
        else:
            filters.append(FFFilterTick.volume == value)
    # Trade Value
    if filter_criteria.get("trdValFrom", None):
        value = filter_criteria["trdValFrom"]
        operator = filter_criteria["trdValOp"].lower()
        if operator == "more":
            filters.append(FFFilterTick.trade_value > value)
        elif operator == "less":
            filters.append(FFFilterTick.trade_value < value)
        else:
            filters.append(FFFilterTick.trade_value == value)
    # OI
    if filter_criteria.get('oiFrom', None):
        filters.append(and_(FFFilterTick.oi > filter_criteria["oiFrom"], FFFilterTick.oi < filter_criteria["oiTo"]))
    # OI_Change
    if filter_criteria.get('oiChangeFrom', None):
        filters.append(and_(FFFilterTick.oi > filter_criteria["oiChangeFrom"], FFFilterTick.oi < filter_criteria["oiChangeTo"]))
    # OI percent 
    if filter_criteria.get('oiPercentFrom', None):
        filters.append(and_(OI_CAHNGE_PRECENT_CASE > filter_criteria["oiPercentFrom"], OI_CAHNGE_PRECENT_CASE < filter_criteria["oiPercentTo"]))
    #  Volume/OI
    if filter_criteria.get('volOiFrom', None):
        filters.append(and_(VOL_OI_CASE > filter_criteria["volOiFrom"], VOL_OI_CASE < filter_criteria["volOiTo"]))
    # OTM %
    if filter_criteria.get("otm", None):
        value = filter_criteria["otm"].lower()
        if value == "above":
            filters.append(OTM_PERCENT_CASE >= 0)
        else:
            filters.append(OTM_PERCENT_CASE < 0)
    # Cumulative
    if filter_criteria.get("cumulative", None):
        value = filter_criteria["cumulative"].lower()
        if value == "above":
            filters.append(FFFilterTick.delta_volume >= 0)
        else:
            filters.append(FFFilterTick.delta_volume < 0)
    # VolumeDelta
    # VolumeDeltaValue
    query = query.filter(
        and_(*filters)
        ).order_by(FFFilterTick.id.desc()).limit(50)
    
    result = query.all()
    sorted_result = sorted(result, key=lambda x: x.timestamp)
    return serialize_result(sorted_result)

def get_condition_rows(db:Session, from_date:dt, to_date: dt):
    query = db.query(FFFilterTick).filter(
        FFFilterTick.timestamp >= from_date,
        FFFilterTick.timestamp < to_date
    ).order_by(desc(FFFilterTick.id))
    
    return query.all()

def serialize_result(query_result):
    result = []
    industries_str=""
    for row in query_result:
        if row.symbol in SECTOR_SYMBOLS_DF.index:
            sector = SECTOR_SYMBOLS_DF.loc[row.symbol]
            if isinstance(sector['Industry'], pd.Series):  # Case where there are multiple industries
                industries_list = sector['Industry'].str.lower().str.capitalize().drop_duplicates().tolist()
            else:  # Case where there's only one industry
                industries_list = [sector['Industry'].lower().capitalize()]
            industries_str = ", ".join(industries_list)
        row_dict = {
            'id': row.id,
            'symbol_id': row.symbol_id,
            'symbol': row.symbol,
            'exchange_token': row.exchange_token,
            'timestamp': row.timestamp,
            'products': row.products,
            'moneyness': row.moneyness,
            'strike': row.strike,
            'expiry': row.expiry,
            'dte': row.dte,
            'ltp': row.ltp,
            'ltq': row.ltq,
            'side': row.side,
            'flowtype': row.flowtype,
            'volume': row.volume,
            'trd_val': row.trd_val,
            'oi': row.oi,
            'oi_change': row.oi_change,
            'oi_change_percent': row.oi_change_percent,
            'vol_oi': row.vol_oi,
            'underlier_symbol': row.underlier_symbol,
            'otm_percent': row.otm_percent,
            'vol_delta': row.vol_delta,
            'vol_delta_value': row.vol_delta_value,
            'cumulative_delta': row.cumulative_delta,
            "sweep1": row.sweep1,
            "sweep2": row.sweep2,
            "sweep3": row.sweep3,
            "power_sweep": row.power_sweep,
            "block1": row.block1,
            "block2": row.block2,
            "power_block": row.power_block,
            "sector": industries_str
        }
        result.append(row_dict)
    return result