from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(prefix="/download")

@router.get("/tdindex")
def download_tdindex():
    return FileResponse(
        "TDIndexMapping.csv", 
        media_type="text/csv", 
        filename="TDIndexMapping.csv"
    )

@router.get("/vendor")
def download_vendor():
    return FileResponse(
        "VendorsSymbolIDFormat.csv", 
        media_type="text/csv", 
        filename="VendorsSymbolIDFormat.csv"
    )