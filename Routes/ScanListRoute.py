from fastapi import APIRouter

router = APIRouter(tags=["ScanListManagement"], prefix='/scanlist')

@router.get("/getAll")
async def get_all_scan_list(email: str):
    # get UserId by email
    # if don't exist, create new User
    # get all public scan list that userId is not same
    # get all my scan list 
    # combine both all public and mine
    # return combined scan list
    return {
        "message": email
    }

@router.post("/addNew")
async def post_add_new():
    # check the email that exist in user table,
    # if not add email and then return userId
    # check the scan that exist with same title
    # if no continue, if yes, arise error
    # add new condition
    pass