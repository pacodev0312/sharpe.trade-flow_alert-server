from sqlalchemy import create_engine, Integer, Column, String, DateTime, Float, BIGINT, Double, Date, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dependencies import pg_db, pg_host, pg_password, pg_port, pg_username

Base = declarative_base()

class FFFilterTick(Base):
    __tablename__ = "ff_filter_trade"
    
    id=Column(Integer, primary_key=True)
    symbol_id=Column(String, nullable=True)
    timestamp=Column(DateTime, nullable=True)
    ltp=Column(Float, nullable=True)
    bid=Column(Float, nullable=True)
    bid_qty=Column(Integer, nullable=True)
    ask=Column(Float, nullable=True)
    ask_qty=Column(Integer, nullable=True)
    volume=Column(BIGINT, nullable=True)
    oi=Column(Integer, nullable=True)
    last_size=Column(BIGINT, nullable=True)
    lot_size=Column(Integer, nullable=True)
    symbol=Column(String, nullable=True)
    symbol_name=Column(String, nullable=True)
    exchange_token=Column(BIGINT, nullable=True)
    underlier_symbol=Column(String, nullable=True)
    underlier_price=Column(Float, nullable=True)
    trade_value=Column(Double, nullable=True)
    delta_volume=Column(BIGINT, nullable=True)
    delta_volume_value=Column(Double, nullable=True)
    # updated
    buy_volume=Column(BIGINT, nullable=True)
    buy_value = Column(Double, nullable=True)
    sell_volume=Column(BIGINT, nullable=True)
    sell_value=Column(BIGINT, nullable=True)
    #
    symbol_type=Column(String, nullable=True)
    exchange=Column(String, nullable=True)
    tick_size=Column(String, nullable=True)
    expiry=Column(Date, nullable=True)
    option_type=Column(String, nullable=True)
    strike=Column(Integer, nullable=True)
    precision=Column(Float, nullable=True)
    oi_change=Column(Integer, nullable=True)
    moneyness=Column(String, nullable=True)
    # updated
    tag=Column(String, nullable=True)
    aggressor=Column(String, nullable=True)
    
    sweep1=Column(String, nullable=True)
    sweep1_volume=Column(BIGINT, nullable=True)
    sweep1_value=Column(Double, nullable=True)
    
    sweep2=Column(String, nullable=True)
    sweep2_volume=Column(BIGINT, nullable=True)
    sweep2_value=Column(Double, nullable=True)
    
    sweep3=Column(String, nullable=True)
    sweep3_volume=Column(BIGINT, nullable=True)
    sweep3_value=Column(Double, nullable=True)
    
    sweep4=Column(String, nullable=True)
    sweep4_volume=Column(BIGINT, nullable=True)
    sweep4_value=Column(Double, nullable=True)
    
    power_sweep=Column(String, nullable=True)
    power_sweep_volume=Column(BIGINT, nullable=True)
    power_sweep_value=Column(Double, nullable=True)
    
    block1=Column(String, nullable=True)
    block1_volume=Column(BIGINT, nullable=True)
    block1_value=Column(Double, nullable=True)
    
    block2=Column(String, nullable=True)
    block2_volume=Column(BIGINT, nullable=True)
    block2_value=Column(Double, nullable=True)
    
    """If Last Volume > Total of Last 20 Last volumes"""
    power_block=Column(String, nullable=True)
    power_block_volume=Column(BIGINT, nullable=True)
    power_block_value=Column(Double, nullable=True)
    
    """Current Price is 500 % > Previous Tick if price is below 0.25 OR Current Price is 200 % > previous Tick if the price above 0.25 and below 0.50 OR Current Price is 100% > previous Tick if the price is above 0.50 and below 1, OR Current Price is 50% > previous Tick if the price is above 1 and below 2 OR  Current Price is 25% > previous Tick if the price is above 2 and below 5 OR  Current Price is 10% > previous Tick if the price is above 5 and below 10 OR  Current Price is 5% > previous Tick if the price is above 10 and below 50 OR  Current Price is 4% > previous Tick if the price is above 50 and below 100 OR  Current Price is 3% > previous Tick if the price is above 100 and below 200 OR  Current Price is 2% > previous Tick if the price is above 200 and below 500 OR  Current Price is 1% > previous Tick if the price is above 500 AND Current Tick Time and Previous Tick Time > 60 seconds
    """
    unusal_prive_activity=Column(String, nullable=True)
    #
    strike_difference=Column(Float, nullable=True)
    symbol_root=Column(String, nullable=True)
    local_id=Column(String, nullable=True)
    is_contract_active=Column(Integer, nullable=True)
    oi_build_up=Column(String, nullable=True)
    sentiment=Column(String, nullable=True)
    tick_seq=Column(Integer, nullable=False)
    """for one minute tick"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cumulative_delta = 0.0
        
    __table_args__ = (
        Index('format_idx_symbol_timestamp_tickseq', 'symbol', 'timestamp', 'tick_seq', postgresql_using='btree'),
    )

db_url=f"postgresql+psycopg2://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

engine=create_engine(db_url)

# Create a session
Session = sessionmaker(bind=engine)