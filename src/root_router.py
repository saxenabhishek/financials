import datetime
import time
from functools import partial
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader
from jinjax.catalog import Catalog

from src.service.data_ingestion import DataIngestionService
from src.service.transactions import TransactionIndicator, TransactionService
from src.utils import (
    convert_camel_to_title,
    get_all_file_paths,
    get_logger,
    pipe_human_readable_date,
)

router = APIRouter()

log = get_logger(__name__)

jinja_env = Environment(loader=FileSystemLoader("src/templates"))

custom_filters = {}
custom_filters["titleCase"] = convert_camel_to_title
custom_filters["date"] = pipe_human_readable_date
custom_filters["currency"] = lambda value: f"₹ {value:,.2f}"


catalog = Catalog()
catalog.add_folder("src/templates")
catalog.jinja_env.filters.update(custom_filters)
jinja_env.filters.update(custom_filters)


@router.get("/table", response_class=HTMLResponse)
async def render_table_template(request: Request):
    # Create a dummy DataFrame
    df = pd.DataFrame(
        {
            "A": [1, 2, 3, 4, 5],
            "B": ["a", "b", "c", "d", "e"],
            "C": [1.1, 2.2, 3.3, 4.4, 5.5],
        }
    )
    return catalog.render("table", name="Transaction Data", df=df)


@router.post("/submit")
async def call_server(
    id: str = Form(None),
    notes: str = Form(None),
    type: TransactionIndicator = Form(None),
):
    log.info(f"Received orderId: {id} notes: {notes} type: {type}")
    txnSrv = TransactionService()
    res = txnSrv.update_transaction(id, notes, type)
    log.debug(res)
    return {"orderId": id, "notes": notes, "type": type}


@router.get("/cards", response_class=HTMLResponse)
async def render_cards_template(
    request: Request,
    month: Optional[int] = None,
    indicator: Optional[TransactionIndicator] = None,
    phrase: Optional[str] = None,
):
    st = time.time_ns()
    tags = []
    txnSrv = TransactionService()
    if month is not None:
        start_date, end_date = get_start_and_end_for_month(month)
    else:
        start_date = end_date = None

    view_cols = {"TransactionDate": 0, "ClosingBalance": 0, "Category": 0}

    priceHeader = "WithdrawalAmt"
    if indicator is TransactionIndicator.PENDING:
        view_cols["DepositAmt"] = 0

    if phrase:
        view_cols["special.status"] = 0
        view_cols["special.paymentStatus"] = 0
        view_cols["special.dishString"] = 0

    transactions_data = list(
        txnSrv.get_all_transactions(
            view_cols,
            start_date=start_date,
            end_date=end_date,
            indicator=indicator,
            phrase=phrase,
            combine_with_vendor_data=True,
        )
    )

    display_months = get_months()
    month_link_gen = partial(generate_next_link, "/cards", indicator=indicator)

    display_months = [
        dict(link=month_link_gen(month=month["id"]), **month)
        for month in display_months
    ]

    tags.append(f"{len(transactions_data)} Txns")
    tags.append(f"{round((time.time_ns() - st)*1e-6, 3)}ms")
    tags.append("Sorted: Value Date")
    tags.append(
        f"Indicator: {indicator.value}" if indicator is not None else "All Txns"
    )
    if phrase is not None:
        tags.append(f"Phrase: {phrase}")

    return catalog.render(
        "TxnPage",
        request=request,
        heading="_id",
        name="Transaction Data",
        priceHeader=priceHeader,
        indicatorHeader="TransactionIndicator",
        indicatorColors=txnSrv.generate_tailwind_colors(),
        tags=tags,
        months=display_months,
        data=transactions_data,
        selected_month=month if month is not None else -1,
    )


@router.get("/ingest-data", response_class=HTMLResponse)
async def ingest_data(request: Request):
    st = time.time_ns()
    service = DataIngestionService()
    res = service.ingest_data()
    return catalog.render(
        "good",
        title=f"{res} Transactions Ingested in {round((time.time_ns() - st)*1e-9, 3)}secs",
        task="Ingest Data Task",
    )


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, month: Optional[int] = None):
    txnSrv = TransactionService()
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
    }

    if month is not None:
        start_date, end_date = get_start_and_end_for_month(month)
    else:
        start_date = end_date = None

    unread_transactions = get_all_unread_transaction_files()
    last_transaction_date = txnSrv.get_last_transaction_date(start_date, end_date)
    pending_transactions = txnSrv.get_pending_transactions(start_date, end_date)
    split_transactions = txnSrv.get_split_transactions(start_date, end_date)
    settled_transactions = txnSrv.get_settled_transactions(start_date, end_date)
    kpi_link_gen = partial(generate_next_link, "/cards", month=month)

    kpi_data = [
        {
            "name": "Last Transaction Date",
            "value": pipe_human_readable_date(last_transaction_date),
            "color": "orange-500",
            "subtext": f"{calculate_days_since_last_transaction(last_transaction_date)} days since update",
            "link": "/cards",
        },
        {
            "name": "Number Of Pending Transactions",
            "value": len(list(pending_transactions)),
            "color": "yellow-500",
            "link": kpi_link_gen(TransactionIndicator.PENDING),
        },
        {
            "name": "Transactions Waiting to be Split",
            "value": len(list(split_transactions)),
            "color": "red-500",
            "link": kpi_link_gen(TransactionIndicator.NEEDS_SPLIT),
        },
        {
            "name": "Total Settled Transactions",
            "value": len(list(settled_transactions)),
            "color": "purple-500",
            "link": kpi_link_gen(TransactionIndicator.SETTLED),
        },
    ]

    context = dict(
        request=request,
        vendor_metrics=vendor_metrics,
        unread_transactions=unread_transactions,
        kpi_data=kpi_data,
        months=get_months(),
        selected_month=month if month is not None else -1,
    )
    return jinja_env.get_template("LandingPage.jinja").render(context)


def get_all_unread_transaction_files() -> list[str]:
    zomato_files = get_all_file_paths("zomato_orders", ".json")
    all_files = (
        get_all_file_paths(r"bank_transactions\hdfc_data")
        + get_all_file_paths(r"bank_transactions\icici_data")
        + zomato_files
    )
    return [file for file in all_files if "old" not in file]


def calculate_days_since_last_transaction(
    last_transaction_date: datetime.datetime,
) -> int:
    current_date = datetime.datetime.now()
    return (current_date - last_transaction_date).days


def get_start_and_end_for_month(
    month: int,
) -> tuple[datetime.datetime, datetime.datetime]:
    current_year = datetime.datetime.now().year
    start_date = datetime.datetime(current_year, month, 1)
    end_date = datetime.datetime(current_year, month + 1, 1) - datetime.timedelta(
        days=1
    )
    return start_date, end_date


def get_months():
    months_list = [
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
    months_list = months_list[: datetime.datetime.now().month]
    months = [{"id": i + 1, "name": month} for i, month in enumerate(months_list)]
    return months


def generate_next_link(
    route, indicator: Optional[TransactionIndicator], month: Optional[int]
):
    link = route + "?"
    link_args = []
    if month is not None:
        link_args.append(f"month={month}")
    if indicator is not None:
        link_args.append(f"indicator={indicator.value}")
    return link + "&".join(link_args)
