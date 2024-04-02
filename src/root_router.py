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
import datetime
from typing import Optional


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
@router.get("/table", response_class=HTMLResponse)
async def render_template(request: Request):
    zomato_transactions = db.transactions.search(db.Query().ZomatoDictData != "")
    zomato_orders = pd.DataFrame(zomato_transactions)
    context = dict(request=request, name="Zomato Data", df=zomato_orders)
    # return templates.TemplateResponse("table.html", context)
    return jinja_env.get_template("split_transactions.html").render(context)


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
async def dashboard(request: Request, month: Optional[int] = None):
    vendor_metrics = {
        "Zomato": {
            "Total Transactions": 145,
            "Pending Transactions": 25,
            "Settled Transactions": 120,
        },
        "Zepto": {
            "Total Transactions": 78,
            "Pending Transactions": 10,
            "Settled Transactions": 68,
        },
        "Blinkit": {
            "Total Transactions": 92,
            "Pending Transactions": 15,
            "Settled Transactions": 77,
        },
        "Blinkit1": {
            "Total Transactions": 92,
            "Pending Transactions": 15,
            "Settled Transactions": 77,
        },
        "Blinkit2": {
            "Total Transactions": 92,
            "Pending Transactions": 15,
            "Settled Transactions": 77,
        },
    }

    unread_transactions = get_all_unread_transaction_files()

    if month is not None:
        start_date, end_date = get_start_and_end_for_month(month)
    else:
        start_date = end_date = None

    transactions = db.transactions.all()
    filtered_transactions = db.filter_transactions_by_date(
        transactions, start_date, end_date
    )
    _, last_transaction_date_str = db.get_first_and_last_transaction_date(
        filtered_transactions
    )

    kpi_data = [
        {
            "name": "Last Transaction Date",
            "value": pipe_human_readable_date(last_transaction_date_str),
            "color": "orange-500",
            "subtext": f"{calculate_days_since_last_transaction(last_transaction_date_str)} days since update",
        },
        {
            "name": "Number Of Pending Transactions",
            "value": len(
                db.filter_transactions_by_date(
                    db.get_transaction_by_status(db.INDICATOR.PENDING),
                    start_date,
                    end_date,
                )
            ),
            "color": "yellow-500",
        },
        {
            "name": "Transactions Waiting to be Split",
            "value": len(
                db.filter_transactions_by_date(
                    db.get_transaction_by_status(db.INDICATOR.NEEDS_SPLIT),
                    start_date,
                    end_date,
                )
            ),
            "color": "red-500",
        },
        {
            "name": "Total Settled Transactions",
            "value": len(
                db.filter_transactions_by_date(
                    db.get_transaction_by_status(db.INDICATOR.SETTLED),
                    start_date,
                    end_date,
                )
            ),
            "color": "purple-500",
        },
    ]

    months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    months = months[: datetime.datetime.now().month]

    context = dict(
        request=request,
        vendor_metrics=vendor_metrics,
        unread_transactions=unread_transactions,
        kpi_data=kpi_data,
        months=months,
    )
    return jinja_env.get_template("index.html").render(context)


def get_all_unread_transaction_files() -> list[str]:
    return get_all_file_paths(r"bank_transactions\hdfc_data") + get_all_file_paths(
        r"bank_transactions\icici_data"
    )


def calculate_days_since_last_transaction(last_transaction_str: str) -> int:
    last_transaction_date = datetime.datetime.strptime(
        last_transaction_str, "%Y-%m-%d"
    ).date()
    current_date = datetime.datetime.now().date()
    return (current_date - last_transaction_date).days


def pipe_human_readable_date(date_str: str) -> str:
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    return date.strftime("%B %d, %Y")


def get_start_and_end_for_month(month: int) -> tuple[str, str]:
    current_year = datetime.datetime.now().year
    start_date = datetime.datetime(current_year, month, 1).strftime("%Y-%m-%d")
    end_date = (
        datetime.datetime(current_year, month + 1, 1) - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%d")
    return start_date, end_date
