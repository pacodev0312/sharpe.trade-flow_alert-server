from sqlalchemy import desc
from sqlalchemy.orm import Session
from Services.db_connection import FilteringOptions
from datetime import datetime as dt, timezone as tz

def get_options_by_email(db: Session, email: str):
    pass

def get_option_by_id(db: Session, id: int):
    query = db.query(FilteringOptions).where(FilteringOptions.id == id).first()
    return query

def get_options_by_status(db:Session, status):
    pass

def get_all(db: Session):
    return db.query(FilteringOptions).all()

def add_option_by_email(db:Session, email: str, condition: str, status: str, title:str):
    print(condition)
    new_option = FilteringOptions(
        title=title,
        email = email,
        condition = condition,
        createdAt = dt.now(tz.utc).strftime("%Y-%m-%d %H:%M:%S"),
        creater=email,
        status=status
    )
    
    db.add(new_option)
    db.commit()
    return new_option.id

def update_option_by_id(db:Session, email: str, condition: str, status: str, id:int, title:str):
    try:
        # Update the row with the given ID
        db.query(FilteringOptions).filter(FilteringOptions.id == id).update(
            {
                "condition": condition,
                "email": email,
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