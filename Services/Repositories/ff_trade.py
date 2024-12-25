from sqlalchemy import desc
from sqlalchemy.orm import Session, aliased
from datetime import datetime as dt, timedelta
from Services.db_connection import FFFilterTick

def get_last_n_ticks(db: Session):
    subquery = (
        db.query(FFFilterTick)
        .order_by(desc(FFFilterTick.id))
        .limit(5000)
        .subquery()
    )

    # Alias the subquery to treat it as a table
    alias = aliased(FFFilterTick, subquery)

    # Query from the subquery and order by ascending id
    results = db.query(alias).order_by(alias.id.asc()).all()
    return results
    
def get_condition_rows(db:Session, from_date:dt, to_date: dt):
    query = db.query(FFFilterTick).filter(
        FFFilterTick.timestamp >= from_date,
        FFFilterTick.timestamp < to_date
    ).order_by(desc(FFFilterTick.id))
    
    return query.all()