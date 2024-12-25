from fastapi import APIRouter, Query
from Services.db_connection import Session
from Services.Repositories import ff_trade
from datetime import datetime as dt
from typing import Optional

router = APIRouter(
    prefix='',
    tags=["HistoricalRoute"]
)

@router.get('/getLastNTicks')
def get_last_n_ticks(date: Optional[any] = Query(default=None)):
    result =ff_trade.get_last_n_ticks(db=Session())
    
    return result

@router.post('/getConditions')
def get_conditions(conditions:str):
    print(conditions)
    return ""