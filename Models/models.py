import json
from typing import Dict
class OptionStatus:
    Private = "Private"
    Public = "Public"
    
class FilteringResponseModel:
    symbol_id: str
    symbol: str
    timestamp: str
    products:str
    moneyness:str
    strike:int
    expiry:str
    dte:str
    ltp:float
    atp: float
    ltq:int
    side:str
    flowtype:str
    flags: list
    volume:int
    trd_val:float
    oi:float
    oi_change:float
    oi_change_percent:float
    vol_oi:int
    underlier_symbol: str
    underlier_price: float
    otm_percent:float
    atp_vwap:float
    vol_delta:int
    vol_delta_value:float
    cumulative_delta:int
    sweep1: str
    sweep2: str
    sweep3: str
    power_sweep: str
    block1: str
    block2: str
    power_block: str
    sector: str
    change: float
    change_percent: float
    lot_size: int
    token: int
    exchange: str
    underlier_token: int
        
    def to_dict(self) -> Dict[str, any]:
        return {
            "symbol_id": self.symbol_id,
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "products": self.products,
            "moneyness": self.moneyness,
            "strike": self.strike,
            "expiry": self.expiry,
            "dte": self.dte,
            "ltp": self.ltp,
            "atp": self.atp,
            "ltq": self.ltq,
            "side": self.side,
            "flowtype": self.flowtype,
            "volume": self.volume,
            "trd_val": self.trd_val,
            "oi": self.oi,
            "oi_change": self.oi_change,
            "oi_change_percent": self.oi_change_percent,
            "vol_oi": self.vol_oi,
            "underlier_symbol": self.underlier_symbol,
            "underlier_price": self.underlier_price,
            "otm_percent": self.otm_percent,
            "atp_vwap": self.atp_vwap,
            "vol_delta": self.vol_delta,
            "vol_delta_value": self.vol_delta_value,
            "cumulative_delta": self.cumulative_delta,
            "sweep1": self.sweep1,
            "sweep2": self.sweep2,
            "sweep3": self.sweep3,
            "power_sweep": self.power_sweep,
            "block1": self.block1,
            "block2": self.block2,
            "power_block": self.power_block,
            "sector": self.sector,
            "change": self.change,
            "change_percent": self.change_percent,
            "token": self.token,
            "lot_size": self.token,
            "exchange": self.exchange,
            "underlier_token": self.underlier_token
        }
