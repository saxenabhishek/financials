from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader
from src.utils import (
    get_logger,
    pipe_human_readable_date,
    get_all_file_paths,
    convert_camel_to_title,
)

import datetime
from typing import Optional
from src.service.data_ingestion import DataIngestionService
from src.service.transactions import TransactionService, TransactionIndicator
import time

router = APIRouter()

log = get_logger(__name__)

jinja_env = Environment(loader=FileSystemLoader("src/templates"))

jinja_env.filters["titleCase"] = convert_camel_to_title
jinja_env.filters["humanDate"] = pipe_human_readable_date
jinja_env.filters["currency"] = lambda value: f"₹ {value:,.2f}"


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
    if id is not None:
        return {"orderId": id, "notes": notes, "type": type}
    return {"message": "Failed to update transaction"}


@router.get("/cards", response_class=HTMLResponse)
async def render_cards_template(
    request: Request,
    month: Optional[int] = None,
    indicator: Optional[TransactionIndicator] = None,
):
    st = time.time_ns()
    tags = []
    txnSrv = TransactionService()
    if month is not None:
        start_date, end_date = get_start_and_end_for_month(month)
    else:
        start_date = end_date = None

    view_cols = {"TransactionDate": 0, "ClosingBalance": 0, "Category": 0}
    if indicator is TransactionIndicator.PENDING:
        view_cols["DepositAmt"] = 0

    transactions_data = list(
        txnSrv.get_all_transactions(
            view_cols,
            start_date=start_date,
            end_date=end_date,
            indicator=indicator,
        ).sort("ValueDate", 1)
    )

    display_months = get_months()

    tags.append("Sorted by Value Date")
    tags.append(f"{len(transactions_data)} Txns")
    tags.append(f"{round((time.time_ns() - st)*1e-6, 3)}ms")
    # if len(transactions_data) > 0:
    #     tags += [column for column in transactions_data[0].keys()]
    # else:
    #     tags += ["No Data Found"]

    context = dict(
        request=request,
        heading="_id",
        name="Transaction Data",
        priceHeader="WithdrawalAmt",
        indicatorHeader="TransactionIndicator",
        indicatorColors=txnSrv.generate_tailwind_colors(),
        tags=tags,
        months=display_months,
        data=transactions_data,
        selected_month=month if month is not None else -1,
    )
    return jinja_env.get_template("cards.html").render(context)


@router.get("/ingest-data", response_class=HTMLResponse)
async def ingest_data(request: Request):
    st = time.time_ns()
    service = DataIngestionService()
    res = service.ingest_data()
    context = dict(
        request=request,
        title=f"{res} Transactions Ingested in {round((time.time_ns() - st)*1e-9, 3)}secs",
        task="Ingest Data Task",
    )
    return jinja_env.get_template("good.html").render(context)


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
            "link": generate_next_link(TransactionIndicator.PENDING, month),
        },
        {
            "name": "Transactions Waiting to be Split",
            "value": len(list(split_transactions)),
            "color": "red-500",
            "link": generate_next_link(TransactionIndicator.NEEDS_SPLIT, month),
        },
        {
            "name": "Total Settled Transactions",
            "value": len(list(settled_transactions)),
            "color": "purple-500",
            "link": generate_next_link(TransactionIndicator.SETTLED, month),
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
    return jinja_env.get_template("index.html").render(context)


def get_all_unread_transaction_files() -> list[str]:
    all_files = get_all_file_paths(r"bank_transactions\hdfc_data") + get_all_file_paths(
        r"bank_transactions\icici_data"
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
    return months


def generate_next_link(indicator: TransactionIndicator, month: Optional[int]):
    if month is not None:
        return f"/cards?indicator={indicator.value}&month={month}"
    return f"/cards?indicator={indicator.value}"
