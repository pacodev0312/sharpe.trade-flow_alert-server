from fastapi import APIRouter, Query
from Services.db_connection import Session
from Services.Repositories import ff_trade, filtering_option
from Models.models import OptionStatus
from datetime import datetime as dt
from typing import Optional
from dependencies import index_dict

router = APIRouter(
    prefix='',
    tags=["HistoricalRoute"]
)

@router.get('/getLastNTicks')
def get_last_n_ticks(condition: str):
    try:
        condition_dict = dict(item.split(":") for item in condition.split(",") if ':' in item)
        df =ff_trade.get_last_n_ticks(db=Session(), condition=condition_dict)
        return df
    except Exception as ex:
        print(f"HistoricalRoute::get_last_n_ticks Error: {ex}")
        return []

@router.get("/Index")
def get_index():
    industries = [item["Industry"] for item in index_dict]

    unique_industries = []
    seen = set()
    for industry in industries:
        if industry not in seen:
            seen.add(industry)
            unique_industries.append(industry)
    return unique_industries

@router.get('/getConditions/{id}')
def get_conditions(id):
    result = filtering_option.get_option_by_id(db=Session(),id=id)
    return {
        "condition": result.condition,
    }
    
@router.get('/getAllCondition')
def get_all_conditions():
    try:
        result = filtering_option.get_all(db=Session())
        return result
    except Exception as ex:
        print(f"GetAllCondition Error: {ex}")
        return []

@router.post("/postAddNewFilteringOption")
def post_add_new_filtering_option(condition, email):
    print(condition)
    condition_dict = dict(item.split(":") for item in condition.split(",") if ':' in item)
    title = condition_dict.get("title", None)
    id = filtering_option.add_option_by_email(db=Session(), email=email, title=title, condition=condition, status=OptionStatus.Public)
    return {"message": id}

@router.post("/postUpdateFilteringOption/{id}")
def post_add_new_filtering_option(condition, email, id):
    condition_dict = dict(item.split(":") for item in condition.split(",") if ':' in item)
    title = condition_dict.get("title", None)
    id = filtering_option.add_option_by_email(db=Session(), email=email, condition=condition, status=OptionStatus.Public)
    return {"message": id}