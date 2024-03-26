from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from src.utils import get_logger, give_table_context, get_all_file_paths
from src.vendors.zomato.mapper import MapZomatoData
from src.bank_parser.hdfc_parser import HdfcExcelDataReader
from src.bank_parser.icici_parser import IciciExcelDataReader


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
    return templates.TemplateResponse("table.html", context)


@router.get("/cards", response_class=HTMLResponse)
async def render_cards_template(request: Request, orderId: str = Form(None)):
    global zomato_orders
    df = zomato_orders
    # Specify the columns you want to display
    columns_to_display = [
        "restaurantName",
        "ValueDate",
        "dishString",
        "WithdrawalAmt",
        "Narration",
        "orderId",
    ]
    view_df = df[columns_to_display].copy()
    view_df.sort_values(by="ValueDate", inplace=True, ascending=False)
    view_df = view_df.dropna()
    context = dict(
        request=request,
        heading="restaurantName",
        name="Zomato Data",
        priceHeader="WithdrawalAmt",
        colHeads=[column.replace("_", " ").title() for column in df.columns],
        **give_table_context(view_df),
    )
    return templates.TemplateResponse("cards.html", context)


@router.get("/ingest-data")
async def ingest_data():
    # specifically read all the files form bank data and ingest them into the database
    # db.transactions.insert_multiple({"word": "hello"} for _ in range(10))
    hdfc_parser = HdfcExcelDataReader(get_all_file_paths("bank_transactions/hdfc_data"))
    icici_parser = IciciExcelDataReader(
        get_all_file_paths("bank_transactions/icici_data")
    )
    return dict(
        hdfc=hdfc_parser.read_data().columns.tolist(),
        icici=icici_parser.read_data().columns.tolist(),
    )


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # give some stats about the master database and how many transaction are unmarked
    # if there are unread files then show a message to the user
    return templates.TemplateResponse("index.html", {"request": request})
