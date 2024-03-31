from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader
from src.utils import (
    get_logger,
    give_table_context,
    get_all_file_paths,
    convert_camel_to_title,
)
from src.bank_parser.hdfc_parser import HdfcExcelDataReader
from src.bank_parser.icici_parser import IciciExcelDataReader
from src import db
import pandas as pd


router = APIRouter()

log = get_logger(__name__)

jinja_env = Environment(loader=FileSystemLoader("src/templates"))

jinja_env.filters["titleCase"] = convert_camel_to_title


@router.post("/submit")
async def call_server(orderId: str = Form(None)):
    # Do something with the orderId if it is provided
    log.info(f"Received orderId: {orderId}")
    if orderId is not None:
        return {"orderId": orderId}
    else:
        return {"message": "No orderId provided"}


# Define a route to render the template
# @router.get("/table", response_class=HTMLResponse)
# async def render_template(request: Request):
#     global zomato_orders
#     df = zomato_orders
#     zomato_items = mapper.dishes_df
#     # Specify the columns you want to display
#     merged_df = df.merge(
#         zomato_items, on="orderId", how="left", suffixes=("_orders", "_items")
#     )
#     context = dict(request=request, name="Zomato Data", **give_table_context(merged_df))
#     return templates.TemplateResponse("table.html", context)


@router.get("/cards", response_class=HTMLResponse)
async def render_cards_template(request: Request, orderId: str = Form(None)):
    zomato_transactions = db.transactions.search(db.Query().ZomatoDictData != "")
    zomato_orders = pd.DataFrame(zomato_transactions)
    zomato_orders = pd.concat(
        [
            zomato_orders.drop(["ZomatoDictData"], axis=1),
            zomato_orders["ZomatoDictData"].apply(pd.Series),
        ],
        axis=1,
    )

    columns_to_display = [
        "restaurantName",
        "ValueDate",
        "dishString",
        "WithdrawalAmt",
        "Narration",
        "orderId",
        "deliveryAddress",
    ]
    view_df = zomato_orders[columns_to_display].copy()
    view_df.sort_values(by="ValueDate", inplace=True, ascending=False)
    # view_df = view_df.dropna()
    context = dict(
        request=request,
        heading="restaurantName",
        name="Zomato Data",
        priceHeader="WithdrawalAmt",
        colHeads=[column for column in zomato_orders.columns],
        **give_table_context(view_df),
    )
    return jinja_env.get_template("cards.html").render(context)


@router.get("/ingest-data")
async def ingest_data():
    toCSV = True
    # specifically read all the files form bank data and ingest them into the database
    # db.transactions.insert_multiple({"word": "hello"} for _ in range(10))
    hdfc_parser = HdfcExcelDataReader(get_all_file_paths("bank_transactions/hdfc_data"))
    icici_parser = IciciExcelDataReader(
        get_all_file_paths("bank_transactions/icici_data")
    )

    hdfc_df = hdfc_parser.read_data()
    icici_df = icici_parser.read_data()

    if toCSV:
        hdfc_df.to_csv("hdfc_data.csv", index=False)
        icici_df.to_csv("icici_data.csv", index=False)

    # dangerous operation remove later
    db.transactions.truncate()

    db.write_transactions([hdfc_df, icici_df])

    # db.transactions.insert_multiple(hdfc_df.to_dict(orient="records"))
    # db.transactions.insert_multiple(icici_df.to_dict(orient="records"))
    return dict(
        hdfc=hdfc_df.columns.tolist(),
        icici=icici_df.columns.tolist(),
    )


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # give some stats about the master database and how many transaction are unmarked
    # if there are unread files then show a message to the user
    return jinja_env.get_template("index.html").render({"request": request})
