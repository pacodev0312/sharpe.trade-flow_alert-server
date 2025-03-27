from sqlalchemy import desc
from sqlalchemy.orm import Session
from Services.db_connection import FilteringOptions, FlowUser
from datetime import datetime as dt, timezone as tz
from Models.models import OptionStatus

def get_options_by_email(db: Session, email: str):
    pass

def get_option_by_id(db: Session, title: str):
    query = db.query(FilteringOptions).where(FilteringOptions.title == title).first()
    return query

def get_options_by_status(db:Session, status):
    pass

def get_all(db: Session, email: str):
    user_options = db.query(FilteringOptions).filter(FilteringOptions.creater == email)
    all_public = db.query(FilteringOptions).filter(FilteringOptions.creater != email, FilteringOptions.status == OptionStatus.Public)

    combined_query = user_options.union(all_public)  # Combines queries at the DB level
    result = combined_query.all()
    return result

def add_user(db:Session, email):
    new_user = FlowUser(email=email)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user

def get_option_by_title(db:Session, title:str):
    option = db.query(FilteringOptions).filter_by(title=title).first()
    return option
    
def add_option_by_email(db:Session, email: str, condition: str, status=OptionStatus.Private, title=None):
    user = db.query(FlowUser).filter_by(email=email).first()
    if user is None:
        user = add_user(db=db, email=email)
        
    new_option = FilteringOptions(
        title=title,
        condition = condition,
        creater=user.email,
        user_id = user.id,
        status=status
    )
    
    db.add(new_option)
    db.commit()
    db.refresh(new_option)
    return new_option

def update_option_by_id(db:Session, email: str, condition: str, status: str, id:int, title:str):
    try:
        # Update the row with the given ID
        db.query(FilteringOptions).filter(FilteringOptions.id == id).update(
            {
                "condition": condition,
                "creater": email,
                "status": status,
                "title": title
            },
            synchronize_session="fetch"  # Ensures session synchronization
        )
        db.commit()

        # Fetch the updated row
        updated_row = db.query(FilteringOptions).filter(FilteringOptions.id == id).first()
        return updated_row
    except Exception as e:
        db.rollback()
        raise Exception(f"Error during update: {e}")
    
def delete_option_by_id(db: Session, id: int):
    try:
        row = db.query(FilteringOptions).filter(FilteringOptions.id == id).first()
        if row:
            db.delete(row)
            db.commit()
            return {"message": f"Row with id {id} deleted successfully"}
        else:
            return {"error": f"Row with id {id} not found"}
    except Exception as ex:
        db.rollback()
        return {"error": f"Failed to delete row: {ex}"}