from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.utils import get_logger
from src.root_router import router

app = FastAPI()
log = get_logger(__name__)
# Mount static files directory (optional)
app.mount("/static", StaticFiles(directory="src/static"), name="static")
app.include_router(router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.__main__:app", host="127.0.0.1", port=8000, reload=True)
