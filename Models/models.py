import pydantic

class OptionStatus:
    Private = "private"
    Public = "public"
    
class FilteringResponseModel:
    id: int
    symbol_id: str
    symbol: str
    exchange_token: int
    timestamp: str
    products:str
    moneyness:str
    strike:str
    expiry:str
    dte:int
    ltp:float
    ltq:int
    side:str
    flowtype:str
    volume:int
    trd_val:float
    oi:float
    oi_change:float
    oi_change_percent:float
    flags:str
    vol_oi:int
    moneyness:str
    otm_percent:float
    atp_vwap:float
    vol_delta:int
    vol_delta_value:float
    cumulative_delta:int
        
    def to_dict(self):
        pass
