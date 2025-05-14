from fastapi import APIRouter, HTTPException
from Services.db_connection import Session
from Services.Repositories import ff_trade, filtering_option
from Models.models import OptionStatus
from datetime import datetime as dt
from typing import Optional
from dependencies import UNIQUE_INDUSTRIES, INDEX_FILENAME

router = APIRouter(
    prefix='',
    tags=["HistoricalRoute"]
)

allow_email=['rs@quantower.in']

@router.get('/getLastNTicks')
async def get_last_n_ticks(condition: str, interval: Optional[str] = None):
    try:
        condition_dict = dict(item.split(":") for item in condition.split(",") if ':' in item)
        df =ff_trade.get_last_n_ticks(db=Session(), condition=condition_dict)
        return df
    except Exception as ex:
        print(f"HistoricalRoute::get_last_n_ticks Error: {ex}")
        return []

@router.get('/getConditionByTitle')
async def get_conditions(title):
    result = filtering_option.get_option_by_id(db=Session(),title=title)
    if result is None:
        raise HTTPException(status_code=400, detail="The same title already exists")
    return {"option": result}
    
@router.get('/getAllCondition')
async def get_all_conditions(email: str):
    try:
        result = filtering_option.get_all(db=Session(), email=email)
        return result
    except Exception as ex:
        print(f"GetAllCondition Error: {ex}")
        return []

@router.post("/postAddNewFilteringOption")
async def post_add_new_filtering_option(condition: str, email: str):
    
    condition_dict = dict(item.split(":") for item in condition.split(",") if ':' in item)
    title = condition_dict.get("title", None)
    status = condition_dict.get("status", None)
    if status is None:
        status = OptionStatus.Private
        
    if email not in allow_email and status is not OptionStatus.Private:
        raise HTTPException(status_code=400, detail="You can't create public scan")
    
    old_option = filtering_option.get_option_by_title(db=Session(), title=title)
    if old_option:
        raise HTTPException(status_code=400, detail="The same title already exists")
    
    result = filtering_option.add_option_by_email(db=Session(), email=email, title=title, condition=condition, status=status)
    return {"message": "Filtering option added successfully", "option": result}

@router.put("/updateFilteringOption")
async def post_update_filtering_option(condition: str, email: str, id: int):
    try:
        # Convert condition string to dictionary
        condition_dict = dict(item.split(":") for item in condition.split(",") if ':' in item)
        title = condition_dict.get("title")
        status = condition_dict.get("status")
        old_option = filtering_option.get_option_by_title(db=Session(), title=title)
        
        if email not in allow_email and status is not OptionStatus.Private:
            raise HTTPException(status_code=400, detail="You can't create public scan")
        
        if old_option and old_option.id != id:
            raise HTTPException(status_code=400, detail="The same title already exists")
        # Update and return the updated row
        updated_row = filtering_option.update_option_by_id(Session(), email, condition, status, id, title)
        if updated_row is None:
            raise HTTPException(status_code=404, detail="Filtering option not found")
        
        return {"message": "Update successful", "updated_data": updated_row}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

@router.delete("/deleteFilteringOption")
async def delete_filtering_option_by_id(id):
    return filtering_option.delete_option_by_id(db=Session(), id=id)