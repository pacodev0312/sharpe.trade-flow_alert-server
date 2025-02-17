import json
from Models.models import FilteringResponseModel
from datetime import datetime as dt

series_filters = {
    "stocks": {"CE", "PE", "XX"},
    "options": {"CE", "PE"},
    "futures": {"XX"},
}

cache_cumulative = {}

def real_time_filter(condition:str, data:str):
    filter_criteria = dict(item.split(":") for item in condition.split(",") if ':' in item)
    data_dict = json.loads(data)
    res_model = FilteringResponseModel()
    res_model.id = data_dict["id"]
    res_model.timestamp = data_dict["timestamp"]
    if data_dict["symbol_type"] in series_filters["stocks"]:
        res_model.products = "Equity"
    elif data_dict["symbol_type"] == "CE":
        res_model.products = "Call"
    elif data_dict["symbol_type"] == "PE":
        res_model.products = "Put"
    else:
        res_model.products = "Futures"
    res_model.moneyness = data_dict["moneyness"]
    res_model.strike = data_dict["strike"]
    # dte
    expiry_date = dt.strptime("%Y-%m-&d")
    today = dt.today()
    res_model.dte = (expiry_date - today).days
    # expiry
    res_model.expiry = data_dict["expiry"]
    # ltp
    res_model.ltp = data_dict["ltp"]
    # ltq
    res_model.ltq = data_dict["last_size"]
    # side
    res_model.side = data_dict["tag"]
    # flowType
    if res_model.side == "Buy" and res_model.products == "Futures":
        res_model.flowtype = "Bullish"
    if res_model.side == "Buy" and res_model.products == "Call":
        res_model.flowtype = "Bullish"
    if res_model.side == "Buy" and res_model.products =="Equity":
        res_model.flowtype = "Bullish"
    if res_model.side == "Sell" and res_model.products == "Put":
        res_model.flowtype = "Bullish"
    if res_model.side == "Sell" and res_model.products == "Futures":
        res_model.flowtype = "Bearish"
    if res_model.side == "Sell" and res_model.products == "Call":
        res_model.flowtype = "Bearish"
    if res_model.side == "Sell" and res_model.products == "Equity":
        res_model.flowtype = "Bearish"
    if res_model.side == "Buy" and res_model.products == "Put":
        res_model.flowtype = "Bearish"
    # volume
    res_model.volume = data_dict["volume"]
    # trd_val
    res_model.trd_val = data_dict["trade_value"]
    # oi
    res_model.oi = data_dict["oi"]
    # oi change
    res_model.oi_change = data_dict["oi_change"]
    # oi change %
    res_model.oi_change_percent = round((res_model.oi_change / res_model.oi) * 100, 2)
    # flags
    # Volume/OI
    res_model.vol_oi = round((data_dict["volume"] / data_dict["oi"]), 2)
    # moneyness
    res_model.moneyness = data_dict["moneyness"]
    # otm_percent
    if data_dict["underlier_price"] != 0:
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
    res_model.vol_delta = data_dict["vol_delta"]
    # vol_delta_value
    res_model.vol_delta_value = data_dict["vol_delta_value"]
    # cumulative delta
    if cache_cumulative.get(res_model.symbol_id, None):
        cache_cumulative[res_model.symbol_id] = data_dict["delta_volume"]
    else:
        cache_cumulative[res_model.symbol_id] = cache_cumulative[res_model.symbol_id] + data_dict["delta_volume"]
    
    # ---------------------------------
    # Filterin Logic
    # ---------------------------------
        
    # Products
    if filter_criteria.get("products", None):
        selected_products = filter_criteria.get("products", "").lower().split("+")
        if res_model.products.lower() in selected_products:
            return None
    # optionType
    if filter_criteria.get("optionType", None):
        value = filter_criteria["optionType"]
        if value == "Calls":
            if res_model.products != "Call":
                return None
        else:
            if res_model.products != "PE":
                return None
    # moneyness
    if filter_criteria.get("moneyness", None):
        moneyness = filter_criteria.get("moneyness").split("+")
        if data_dict["moneyness"] not in moneyness:
            return None
    # expiry
    # DTE
    if filter_criteria.get("dteFrom", None) is not None:
        if res_model.dte < filter_criteria["ltpFrom"] or res_model.dte > filter_criteria["ltpTo"]:
            return None
    # Last
    if filter_criteria.get("ltpFrom", None) is not None:
        if res_model.ltp < filter_criteria["ltpFrom"] or res_model.ltp > filter_criteria["ltpTo"]:
            return None
    # Trade Size
    if filter_criteria.get("ltqFrom", None) is not None:
        if res_model.ltq < filter_criteria["ltqFrom"] or res_model.ltq > filter_criteria["ltqTo"]:
            return None
    # Side
    if filter_criteria.get("side", None) is not None:
        if filter_criteria["side"].lower() != res_model.side.lower():
            return None
    # FlowType
    if filter_criteria.get("flowtype", None):
        if filter_criteria["flowtype"].lower() != res_model.flowtype.lower():
            return None
    # Flags
    if filter_criteria.get("sweep", None):
        value = f"""{filter_criteria["sweep"]}Sweep"""
        if data_dict["sweep1"] != value and data_dict["sweep2"] != value and data_dict["sweep3"] != value:
            return None

    if filter_criteria.get("powerSweep", None):
        value = f"""{filter_criteria["powerSweep"]}Sweep"""
        if data_dict["power_sweep"] != value:
            return None

    if filter_criteria.get("block", None):
        if res_model.side != filter_criteria["block"]:
            return None
        elif data_dict["block1"] != "Block" and data_dict["block2"] != "Block":
            return None

    if filter_criteria.get("powerBlock", None):
        if res_model.side != filter_criteria["powerBlock"]:
            return None
        elif data_dict["power_block"] != "Block":
            return None
    # Volume
    if filter_criteria.get("volumeFrom", None) is not None:
        value = filter_criteria["volumeFrom"]
        operator = filter_criteria["volumeOp"].lower()
        if operator == "more" and res_model.volume < value:
            return None
        if operator == "less" and res_model.volume > value:
            return None
        if operator == "equal" and res_model.volume != value:
            return None
    # TradeValue
    if filter_criteria.get("trdValFrom", None) is not None:
        value = filter_criteria["trdValFrom"]
        operator = filter_criteria["trdValOp"]
        if res_model.trd_val < filter_criteria["trdValFrom"]:
            return None
    # OpenInterest
    if filter_criteria.get("oiFrom", None) is not None:
        if res_model.oi < filter_criteria["oiFrom"] or res_model.oi > filter_criteria["oiTo"]:
            return None
    # OI Change
    if filter_criteria.get("oiChangeFrom", None) is not None:
        if res_model.oi_change < filter_criteria["oiChangeFrom"] or res_model.oi_change > filter_criteria["oiChnageTo"]:
            return None
    # OI Change %
    if filter_criteria.get("oiPercentFrom", None) is not None:
        if res_model.oi_change_percent < filter_criteria["oiPercentFrom"] or res_model.oi_change_percent > filter_criteria["oiPercentTo"]:
            return None
    # Volume/OI
    if filter_criteria.get("volOiFrom", None) is not None:
        if res_model.vol_oi < filter_criteria["volOiFrom"] or res_model.vol_oi > filter_criteria["volOiTo"]:
            return None
    # ATP/vWAP
    if filter_criteria.get("atpVwap", None) is not None:
        value= filter_criteria["atpVwap"].lower()
        if value == "above" and res_model.atp_vwap < 0:
            return None
        if value == "below" and res_model.atp_vwap > 0:
            return None
    # OTM %
    if filter_criteria.get("otm", None) is not None:
        value= filter_criteria["otm"].lower()
        if value == "above" and res_model.otm_percent < 0:
            return None
        if value == "below" and res_model.otm_percent > 0:
            return None
    # Cumlative Delta
    if filter_criteria.get("cumulative", None) is not None:
        value= filter_criteria["cumulative"].lower()
        if value == "positive" and res_model.cumulative_delta < 0:
            return None
        if value == "negative" and res_model.cumulative_delta > 0:
            return None
    return data