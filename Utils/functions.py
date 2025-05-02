import json
from Models.models import FilteringResponseModel
from datetime import datetime as dt
from dependencies import PREDEFINED_WATCHLIST, INDEX_FILENAME, UNIQUE_INDUSTRIES, SECTOR_SYMBOLS, SECTOR_SYMBOLS_DF
import pandas as pd

series_filters = {
    "stocks": {"CE", "PE", "XX"},
    "options": {"CE", "PE"},
    "futures": {"XX"},
}

product_filters = {
    "stocks": {"equity"},
    "options": {"call", "put"},
    "futures": {"futures"},
    "indices": {"index"}
}

cache_cumulative = {}

def real_time_filter(condition:str, data_dict):
    filter_criteria = dict(item.split(":") for item in condition.split(",") if ':' in item)
    # data_dict = json.loads(data)
    res_model = FilteringResponseModel()
    res_model.timestamp = data_dict["timestamp"]
    res_model.symbol_id = str(data_dict["symbol_id"])
    res_model.symbol = data_dict["symbol"]
    # if data_dict["exchange"] == "MCX":
    #     return None
    if data_dict["symbol_type"] not in series_filters["stocks"]:
        res_model.products = "Equity"
        if data_dict["symbol_type"] == "IN":
            res_model.products = "Index"
    elif data_dict["symbol_type"] == "CE":
        res_model.products = "Call"
    elif data_dict["symbol_type"] == "PE":
        res_model.products = "Put"
    else:
        res_model.products = "Futures"
    res_model.moneyness = data_dict["moneyness"]
    res_model.strike = data_dict.get("strike")
    res_model.token = data_dict.get("exchange_token")
    res_model.lot_size = data_dict.get("lot_size")
    res_model.exchange = data_dict.get("exchange")
    if res_model.strike is None:
        res_model.strike = 0
    # dte
    if data_dict["expiry"]:
        if data_dict["expiry"] == "0001-01-01T00:00:00":
            res_model.dte = ""
        else:
            expiry_date = dt.strptime(data_dict["expiry"], "%Y-%m-%dT%H:%M:%Sz")
            today = dt.today()
            res_model.dte = (expiry_date.date() - today.date()).days
    else:
        res_model.dte = ""
    # expiry
    res_model.expiry = data_dict["expiry"]
    # ltp
    res_model.ltp = data_dict["ltp"]
    # atp
    res_model.atp = data_dict["atp"]
    # ltq
    res_model.ltq = data_dict["last_size"]
    # side
    res_model.side = data_dict["tag"]
    # aggressor
    res_model.aggressor = data_dict["aggressor"]
    # flowType
    stock_list = ["Equity", "Index"]
    if res_model.side == "Buy" and res_model.products == "Futures":
        res_model.flowtype = "Bullish"
    elif res_model.side == "Buy" and res_model.products == "Call":
        res_model.flowtype = "Bullish"
    elif res_model.side == "Buy" and res_model.products in stock_list:
        res_model.flowtype = "Bullish"
    elif res_model.side == "Sell" and res_model.products == "Put":
        res_model.flowtype = "Bullish"
    elif res_model.side == "Sell" and res_model.products == "Futures":
        res_model.flowtype = "Bearish"
    elif res_model.side == "Sell" and res_model.products == "Call":
        res_model.flowtype = "Bearish"
    elif res_model.side == "Sell" and res_model.products in stock_list:
        res_model.flowtype = "Bearish"
    elif res_model.side == "Buy" and res_model.products == "Put":
        res_model.flowtype = "Bearish"
    else:
        res_model.flowtype = ""
    # volume
    res_model.volume = data_dict["volume"]
    # trd_val
    res_model.trd_val = data_dict["trade_value"]
    # oi
    res_model.oi = data_dict["oi"]
    # oi change
    res_model.oi_change = data_dict["oi_change"]
    # oi change %
    if data_dict["prev_day_oi"] != 0:
        res_model.oi_change_percent = round((res_model.oi_change / data_dict["prev_day_oi"]) * 100, 2)
    else:
        res_model.oi_change_percent = 0
    # flags
    res_model.flags = []
    sweep1 = data_dict.get('sweep1', None)
    sweep2 = data_dict.get('sweep2', None)
    sweep3 = data_dict.get('sweep3', None)
    sweep4 = data_dict.get('sweep4', None)
    power_sweep = data_dict.get('power_sweep', None)
    if power_sweep is not None:
        power_sweep = f"Power{power_sweep}"
    block1 = data_dict.get('block1', None)
    if block1 is not None:
        block1 = f"{res_model.aggressor}{block1}"
    block2 = data_dict.get('block2', None)
    if block2 is not None:
        block2 = f"{res_model.aggressor}{block2}"
    power_block = data_dict.get('block2', None)
    if power_block is not None:
        power_block = f"Power{res_model.aggressor}{power_block}"
        
    if power_sweep in ("PowerBuySweep", "PowerSellSweep"):
        res_model.selected_sweep = power_sweep
        res_model.selected_sweep_number = 5
    else:
        sweep_map = {
            4: sweep4,
            3: sweep3,
            2: sweep2,
            1: sweep1
        }

        for number, sweep in sweep_map.items():
            if sweep in ("BuySweep", "SellSweep"):
                res_model.selected_sweep = sweep
                res_model.selected_sweep_number = number
                break
        else:
            res_model.selected_sweep = None
            res_model.selected_sweep_number = None
            
    if power_block in ("PowerBuyBlock", "PowerSellBlock"):
        res_model.selected_block = power_sweep
        res_model.selected_block_number = 3
    else:
        block_map = {
            2: block2,
            1: block1
        }

        for number, block in block_map.items():
            if block in ("BuyBlock", "SellBlock"):
                res_model.selected_block = block
                res_model.selected_block_number = number
                break
        else:
            res_model.selected_block = None
            res_model.selected_block_number = None
            
    res_model.sector=""
    if res_model.symbol in SECTOR_SYMBOLS_DF.index:
        sector = SECTOR_SYMBOLS_DF.loc[res_model.symbol]
        if isinstance(sector['Industry'], pd.Series):  # Case where there are multiple industries
            industries_list = sector['Industry'].str.lower().str.capitalize().drop_duplicates().tolist()
        else:  # Case where there's only one industry
            industries_list = [sector['Industry'].lower().capitalize()]
        res_model.sector = ", ".join(industries_list)
    # Volume/OI
    if data_dict["oi"] != 0:
        res_model.vol_oi = round((data_dict["volume"] / data_dict["oi"]), 2)
    else:
        res_model.vol_oi = 0
    # underlier
    res_model.underlier_symbol = data_dict['underlier_symbol']
    res_model.underlier_price = data_dict['underlier_price']
    # moneyness
    res_model.moneyness = data_dict["moneyness"]
    # otm_percent
    if data_dict["underlier_price"] and data_dict["strike"] and data_dict["underlier_price"] != 0:
        if res_model.products == "Call":
            res_model.otm_percent = round(((data_dict["strike"] - data_dict["underlier_price"]) / data_dict["underlier_price"]) * 100, 2)
        elif res_model.products == "Put":
            res_model.otm_percent = round(((data_dict["underlier_price"] - data_dict["strike"]) / data_dict["underlier_price"]) * 100, 2)
        else:
            res_model.otm_percent = 0
    else:
        res_model.otm_percent = 0
    # ATP/vWAP
    res_model.atp_vwap = 0
    # vol_delta
    res_model.vol_delta = data_dict["delta_volume"]
    # vol_delta_value
    res_model.vol_delta_value = data_dict["delta_volume_value"]
    # cumulative delta
    if res_model.symbol_id not in cache_cumulative:
        cache_cumulative[res_model.symbol_id] = data_dict["delta_volume"]
        res_model.cumulative_delta = data_dict["delta_volume"]
    else:
        cache_cumulative[res_model.symbol_id] += data_dict["delta_volume"]
        res_model.cumulative_delta = cache_cumulative[res_model.symbol_id]
    res_model.cumulative_delta = round(res_model.cumulative_delta, 0)
    # change
    if data_dict["prev_day_close"] is not None:
        res_model.change = data_dict["ltp"] - data_dict["prev_day_close"]
    else:
        res_model.change = 0
    # change %
    if data_dict["prev_day_close"] != 0:
        res_model.change_percent = round(res_model.change / data_dict["prev_day_close"] * 100, 2)
    else:
        res_model.change_percent = 0
    res_model.underlier_token=data_dict.get("underlier_token")
    
    # ---------------------------------
    # Filterin Logic
    # ---------------------------------
    
    filter_symbols = []
    filter_tokens: list[int] = []
    # Symbols
    if filter_criteria.get("symbols", None):
        symbols = filter_criteria["symbols"].split("+")
        for symbol in symbols:
            filter_symbols.append(symbol)
            
    symbols_tokens_raw = filter_criteria.get("symbolsTokens")
    if isinstance(symbols_tokens_raw, str):
        symbol_tokens = symbols_tokens_raw.split("+")
        filter_tokens = [int(token) for token in symbol_tokens]
    # Pre-defined Watchlist
    if filter_criteria.get("preWatchlist", None):
        pre_watchlists = []
        no_pre_watchlist = filter_criteria.get("preWatchlist").split("+")
        for index in no_pre_watchlist:
            pre_watchlists.append(INDEX_FILENAME[int(index)])
        for pre_watchlist in pre_watchlists:
            for item in PREDEFINED_WATCHLIST[pre_watchlist]:
                filter_symbols.append(item["Symbol"])
    # Sectors
    if filter_criteria.get("sectors", None):
        industries = []
        no_industries = filter_criteria.get("sectors").split("+")
        for index in no_industries:
            industries.append(UNIQUE_INDUSTRIES[int(index)])
        for industry in industries:
            for item in SECTOR_SYMBOLS:
                if item["Industry"] == industry:
                    filter_symbols.append(item["Symbol"])
    symbol_filter_flg = False
    if len(filter_symbols) > 0 :
        if res_model.symbol not in filter_symbols:
            symbol_filter_flg = True
            
    if len(filter_tokens) > 0:
        if res_model.token not in filter_tokens:
            symbol_filter_flg = True
        else:
            print(res_model.token)
    if symbol_filter_flg:
        return None
    # Underliers
    flg_underlier_filter = False
    if filter_criteria.get("underliers", None):
        underliers = filter_criteria["underliers"].split("+")
        if res_model.underlier_symbol not in underliers:
            flg_underlier_filter = True
    
    if filter_criteria.get("underliersTokens", None):
        underlier_tokens_raw = filter_criteria["underliersTokens"].split("+")
        underlier_tokens = [int(token) for token in underlier_tokens_raw]
        if res_model.underlier_token not in underlier_tokens:
            flg_underlier_filter = True
    if filter_criteria.get("underWatchlist", None):
        underliers = []
        pre_watchlists = []
        no_pre_watchlist = filter_criteria.get("underWatchlist").split("+")
        for index in no_pre_watchlist:
            pre_watchlists.append(INDEX_FILENAME[int(index)])
        for pre_watchlist in pre_watchlists:
            for item in PREDEFINED_WATCHLIST[pre_watchlist]:
                underliers.append(item["Symbol"])
        if res_model.underlier_symbol not in underliers:
            return None
    if flg_underlier_filter:
        return None
    # Products
    if filter_criteria.get("products", None):
        selected_products = filter_criteria.get("products").lower().split("+")
        filter_product_sets = set()
        for values in selected_products:
            filter_product_sets.update(product_filters[values])
        if res_model.products.lower() not in filter_product_sets:
            return None
        
    # optionType
    if filter_criteria.get("optionType", None):
        value = filter_criteria["optionType"]
        optionTypes = value.split("+")
        if res_model.products not in optionTypes:
            return None
    # moneyness
    if filter_criteria.get("moneyness", None):
        moneyness = filter_criteria.get("moneyness").split("+")
        if data_dict["moneyness"] is None:
            return None
        if data_dict["moneyness"].lower() not in moneyness:
            return None
    # expiry
    # DTE
    if filter_criteria.get("dteOp", None):
        op = filter_criteria["dteOp"]
        if res_model.dte == "":
            return None
        dte = int(res_model.dte)
        if op == "Range":
            rangeFrom = int(filter_criteria["dteFrom"])
            rangeTo = int(filter_criteria["dteTo"])
            if dte < rangeFrom or dte > rangeTo:
                return None
        else:
            value = int(filter_criteria["dteValue"])
            if op == "More" and (dte) <= value:
                return None
            if op == "Less" and dte >= value:
                return None
            if op == "Equal" and dte != value:
                return None
                
    # Last
    if filter_criteria.get("ltpOp", None):
        op = filter_criteria["ltpOp"]
        if op == "Range":
            rangeFrom = float(filter_criteria["ltpFrom"])
            rangeTo = float(filter_criteria["ltpTo"])
            if res_model.ltp < rangeFrom or res_model.ltp > rangeTo:
                return None
        else:
            value = float(filter_criteria["ltpValue"])
            if op == "More" and res_model.ltp <= value:
                return None
            if op == "Less" and res_model.ltp >= value:
                return None
            if op == "Equal" and res_model.ltp != value:
                return None
    # Trade Size
    if filter_criteria.get("ltqOp", None):
        op = filter_criteria["ltqOp"]
        if op == "Range":
            rangeFrom = float(filter_criteria["ltqFrom"])
            rangeTo = float(filter_criteria["ltqTo"])
            if res_model.ltq < rangeFrom or res_model.ltq > rangeTo:
                return None
        else:
            value = float(filter_criteria["ltqValue"])
            if op == "More" and res_model.ltq <= value:
                return None
            if op == "Less" and res_model.ltq >= value:
                return None
            if op == "Equal" and res_model.ltq != value:
                return None
    # Side
    if filter_criteria.get("side", None) is not None:
        if res_model.side is None:
            return None
        if filter_criteria["side"].lower() != res_model.side.lower():
            return None
    # FlowType
    if filter_criteria.get("flowtype", None):
        flowtypes = filter_criteria["flowtype"].split("+")
        if res_model.flowtype not in flowtypes:
            return None
    # Flags
    conditions_met = False  # Track if at least one condition is satisfied

    sweep_filter = filter_criteria.get("sweep")
    sweep_star_filter = filter_criteria.get("sweepStar")
    selected_sweep = res_model.selected_sweep
    selected_sweep_star = res_model.selected_sweep_number

    if sweep_filter and sweep_filter is not "":
        allowed_sweeps = {f"{sweep}Sweep" for sweep in sweep_filter.split("+") if sweep}

        if sweep_star_filter:
            # Case 3: Both filters provided
            allowed_sweep_stars = sweep_star_filter.split("+")
            if selected_sweep in allowed_sweeps and str(selected_sweep_star) in allowed_sweep_stars:
                conditions_met = True

        elif selected_sweep in allowed_sweeps:
            # Case 1: Only sweep filter
            conditions_met = True

    elif sweep_star_filter:
        # Case 2: Only sweep star filter (sweep filter missing or empty)
        allowed_sweep_stars = sweep_star_filter.split("+")
        if str(selected_sweep_star) in allowed_sweep_stars:
            print(selected_sweep_star, allowed_sweep_stars)
            conditions_met = True
                
    if filter_criteria.get("powerSweep"):
        power_sweeps = {f"Power{item}Sweep" for item in filter_criteria["powerSweep"].split("+")}
        if selected_sweep in power_sweeps:
            conditions_met = True

    block_filter = filter_criteria.get("block")
    block_star_filter = filter_criteria.get("blockStar")
    selected_block = res_model.selected_block
    selected_block_star = res_model.selected_block_number
    aggressor_block = f"{res_model.aggressor}{selected_block}" if res_model.aggressor and selected_block else None

    if block_filter and block_filter is not "":
        allowed_blocks = {f"{block}Block" for block in block_filter.split("+") if block}

        if block_star_filter:
            # Case 3: Both filters
            allowed_block_stars = block_star_filter.split("+")
            if (selected_block in allowed_blocks or aggressor_block in allowed_blocks) and str(selected_block_star) in allowed_block_stars:
                conditions_met = True

        elif selected_block in allowed_blocks or aggressor_block in allowed_blocks:
            # Case 1: Only block filter
            conditions_met = True

    elif block_star_filter:
        # Case 2: Only block star filter (block filter missing or empty)
        allowed_block_stars = block_star_filter.split("+")
        if str(selected_block_star) in allowed_block_stars:
            conditions_met = True

    power_block_filter = filter_criteria.get("powerBlock")
    if power_block_filter:
        allowed_power_blocks = {f"Power{block}Block" for block in power_block_filter.split("+")}
        if res_model.selected_block in allowed_power_blocks:
            conditions_met = True

    if filter_criteria.get("sweep") or filter_criteria.get("powerSweep") or block_filter or filter_criteria.get("powerBlock") or block_star_filter or sweep_star_filter:
        if not conditions_met:
            return None
    
    # Volume
    if filter_criteria.get("volumeOp", None):
        op = filter_criteria["volumeOp"]
        if op == "Range":
            rangeFrom = float(filter_criteria["volumeFrom"])
            rangeTo = float(filter_criteria["volumeTo"])
            if res_model.volume < rangeFrom or res_model.volume > rangeTo:
                return None
        else:
            value = float(filter_criteria["volumeValue"])
            if op == "More" and res_model.volume <= value:
                return None
            if op == "Less" and res_model.volume >= value:
                return None
            if op == "Equal" and res_model.volume != value:
                return None
    # TradeValue
    if filter_criteria.get("trdValOp", None):
        op = filter_criteria["trdValOp"]
        if op == "Range":
            rangeFrom = float(filter_criteria["trdValFrom"])
            rangeTo = float(filter_criteria["trdValTo"])
            if res_model.trd_val < rangeFrom or res_model.trd_val > rangeTo:
                return None
        else:
            value = float(filter_criteria["trdValValue"])
            if op == "More" and res_model.trd_val <= value:
                return None
            if op == "Less" and res_model.trd_val >= value:
                return None
            if op == "Equal" and res_model.trd_val != value:
                return None
    # ATP/LTP
    if filter_criteria.get("atpLtp", None):
        value = filter_criteria["atpLtp"]
        if value == "Above LTP" and res_model.ltp > res_model.atp:
            return None
        if value == "Below LTP" and res_model.ltp < res_model.atp:
            return None
    # OpenInterest
    if filter_criteria.get("oiOp", None):
        op = filter_criteria["oiOp"]
        if op == "Range":
            rangeFrom = float(filter_criteria["oiFrom"])
            rangeTo = float(filter_criteria["oiTo"])
            if res_model.oi < rangeFrom or res_model.oi > rangeTo:
                return None
        else:
            value = float(filter_criteria["oiValue"])
            if op == "More" and res_model.oi <= value:
                return None
            if op == "Less" and res_model.oi >= value:
                return None
            if op == "Equal" and res_model.oi != value:
                return None
    # OI Change
    if filter_criteria.get("oiChangeOp", None):
        op = filter_criteria["oiChangeOp"]
        if op == "Range":
            rangeFrom = float(filter_criteria["oiChangeFrom"])
            rangeTo = float(filter_criteria["oiChangeTo"])
            if res_model.oi_change < rangeFrom or res_model.oi_change > rangeTo:
                return None
        else:
            value = float(filter_criteria["oiChangeValue"])
            if op == "More" and res_model.oi_change <= value:
                return None
            if op == "Less" and res_model.oi_change >= value:
                return None
            if op == "Equal" and res_model.oi_change != value:
                return None
    # OI Change %
    if filter_criteria.get("oiPercentOp", None):
        op = filter_criteria["oiPercentOp"]
        if op == "Range":
            rangeFrom: float = float(filter_criteria["oiPercentFrom"])
            rangeTo: float = float(filter_criteria["oiPercentTo"])
            if res_model.oi_change_percent < rangeFrom or res_model.oi_change_percent > rangeTo:
                return None
        else:
            value: float = float(filter_criteria["oiPercentValue"])
            if op == "More" and res_model.oi_change_percent <= value:
                return None
            if op == "Less" and res_model.oi_change_percent >= value:
                return None
            if op == "Equal" and res_model.oi_change_percent != value:
                return None
    # Volume/OI
    if filter_criteria.get("volOiOp", None):
        op = filter_criteria["volOiOp"]
        if op == "Range":
            rangeFrom: float = float(filter_criteria["volOiFrom"])
            rangeTo: float = float(filter_criteria["volOiTo"])
            if res_model.vol_oi < rangeFrom or res_model.vol_oi > rangeTo:
                return None
        else:
            value: float = float(filter_criteria["volOiValue"])
            if op == "More" and res_model.vol_oi <= value:
                return None
            if op == "Less" and res_model.vol_oi >= value:
                return None
            if op == "Equal" and res_model.vol_oi != value:
                return None
                
    # ATP/vWAP
    if filter_criteria.get("atpVwap", None) is not None:
        value= filter_criteria["atpVwap"].lower()
        if value == "above" and res_model.atp_vwap < 0:
            return None
        if value == "below" and res_model.atp_vwap > 0:
            return None
    # OTM %
    if filter_criteria.get("otmOp", None):
        op = filter_criteria["otmOp"]
        if op == "Range":
            rangeFrom: float = float(filter_criteria["otmFrom"])
            rangeTo: float = float(filter_criteria["otmTo"])
            if res_model.otm_percent < rangeFrom or res_model.otm_percent > rangeTo:
                return None
        else:
            value: float = float(filter_criteria["otmValue"])
            if op == "More" and res_model.otm_percent <= value:
                return None
            if op == "Less" and res_model.otm_percent >= value:
                return None
            if op == "Equal" and res_model.otm_percent != value:
                return None
    # Cumlative Delta
    if filter_criteria.get("cumulative", None) is not None:
        value= filter_criteria["cumulative"].split("+")
        if "Positive" in value and "Negative" in value:
            if res_model.cumulative_delta == 0:
                return None
        else:
            if "Positive" in value:
                if res_model.cumulative_delta <= 0:
                    return None
            if "Negative" in value:
                if res_model.cumulative_delta >= 0:
                    return None
    # Change
    if filter_criteria.get("changeOp", None):
        op = filter_criteria["changeOp"]
        if op == "Range":
            rangeFrom: float = float(filter_criteria["changeFrom"])
            rangeTo: float = float(filter_criteria["changeTo"])
            if res_model.change < rangeFrom or res_model.change > rangeTo:
                return None
        else:
            value: float = float(filter_criteria["changeValue"])
            if op == "More" and res_model.change <= value:
                return None
            if op == "Less" and res_model.change >= value:
                return None
            if op == "Equal" and res_model.change != value:
                return None
    # Change %
    if filter_criteria.get("changePercentOp", None):
        op = filter_criteria["changePercentOp"]
        if op == "Range":
            rangeFrom: float = float(filter_criteria["changePercentFrom"])
            rangeTo: float = float(filter_criteria["changePercentTo"])
            if res_model.change_percent < rangeFrom or res_model.change_percent > rangeTo:
                return None
        else:
            value: float = float(filter_criteria["changePercentValue"])
            if op == "More" and res_model.change_percent <= value:
                return None
            if op == "Less" and res_model.change_percent >= value:
                return None
            if op == "Equal" and res_model.change_percent != value:
                return None   
    return res_model.to_dict()