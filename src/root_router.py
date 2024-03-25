from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from src.utils import get_logger, give_table_context
from src.mappers.zomato.mapper import MapZomatoData


router = APIRouter()
templates = Jinja2Templates(directory="src/templates")
log = get_logger(__name__)

# Load the data
mapper = MapZomatoData()
zomato_orders, _ = mapper.doMapping()

@router.post("/submit")
async def call_server(orderId: str = Form(None)):
    # Do something with the orderId if it is provided
    log.info(f"Received orderId: {orderId}")
    if orderId is not None:
        return {"orderId": orderId}
    else:
        return {"message": "No orderId provided"}


# Define a route to render the template
@router.get("/table", response_class=HTMLResponse)
async def render_template(request: Request):
    global zomato_orders
    df = zomato_orders
    zomato_items = mapper.dishes_df
    # Specify the columns you want to display
    merged_df = df.merge(
        zomato_items, on="orderId", how="left", suffixes=("_orders", "_items")
    )
    context = dict(request=request, name="Zomato Data", **give_table_context(merged_df))
    return templates.TemplateResponse("index.html", context)


@router.get("/cards", response_class=HTMLResponse)
async def render_cards_template(request: Request, orderId: str = Form(None)):
    global zomato_orders
    df = zomato_orders
    # Specify the columns you want to display
    columns_to_display = [
        "restaurantName",
        "Value Date",
        "dishString",
        "Withdrawal Amount (INR )",
        "Narration",
        "orderId",
    ]
    view_df = df[columns_to_display].copy()
    view_df.sort_values(by="Value Date", inplace=True, ascending=False)
    view_df = view_df.dropna()
    return templates.TemplateResponse(
        "cards.html",
        {
            "request": request,
            "df": view_df,
            "columns": view_df.columns.tolist(),
            "name": "Zomato Data",
            "heading": "restaurantName",
            "priceHeader": "Withdrawal Amount (INR )",
            "colHeads": [column.replace("_", " ").title() for column in df.columns],
        },
    )



