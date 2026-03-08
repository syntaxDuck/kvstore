from fastapi import APIRouter


router = APIRouter(prefix="/node", tags=["node"])


@router.post("/set")
def set_value():
    pass


@router.get("/get")
def get_value():
    pass


@router.get("/state")
def get_state():
    pass
