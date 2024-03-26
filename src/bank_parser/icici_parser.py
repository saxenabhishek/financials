import pandas as pd
from src.utils import get_logger
import re

log = get_logger(__name__)


class IciciExcelDataReader:
    def __init__(self, file_paths: list[str]):
        self.file_paths = file_paths
        self.file_path = self.file_paths[0]

    def read_data(self, sheet_name=0) -> pd.DataFrame:
        """
        Read data from the specified sheet of the Excel file, starting from
        the row after the header row,
        and ending at the row before the row containing stars after the table.

        Args:
        - sheet_name (str or int, default 0): Name or index of the sheet to
        read.

        Returns:
        - DataFrame containing the data from the specified sheet, delimited by
        rows containing stars.
        """
        try:
            all_data = self._read_all_data(sheet_name)
            start_row = self._find_start_row(all_data)
            log.debug(f"Start row: {start_row}")
            end_row = self._find_end_row(all_data, (start_row or 0) + 1)
            log.debug(f"End row: {end_row}")

            if start_row is not None and end_row is not None:
                df = self._extract_table_data(sheet_name, start_row, end_row)
                return df
            else:
                raise ValueError("Could not find start and/or end row of the table.")
        except Exception as e:
            raise Exception(f"An error occurred while reading the Excel file: {e}")

    def _extract_narration_info(self, narration):
        # Define patterns for different types of transactions (and they may expand)
        patterns = {
            "UPI": r"UPI/(\d+)/(.*?)\/([^\/]+)@?\/([^\/]+)",
            "NEFT": r"NEFT-(.*?)-(.*?)-(.*)",
        }

        extracted_info = {}

        # Iterate through patterns and extract information
        for key, pattern in patterns.items():
            match = re.search(pattern, narration)
            if match:
                extracted_info["type"] = key
                if key == "UPI":
                    extracted_info["transaction_id"] = match.group(1)
                    extracted_info["message"] = match.group(2)
                    extracted_info["sender_receiver_details"] = match.group(3)
                    extracted_info["bank_name"] = match.group(4)
                elif key == "NEFT":
                    extracted_info["sender_bank"] = match.group(1)
                    extracted_info["transaction_id"] = match.group(2)
                    extracted_info["details"] = match.group(3)
        if len(extracted_info) == 0:
            log.debug(f"No pattern matched for narration: {narration}")
        return extracted_info

    def _read_all_data(self, sheet_name):
        return pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)

    def _find_start_row(self, all_data):
        for index, row in all_data.iterrows():
            if row.astype(str).str.contains("S No.").any():
                return index  # Start from the row after the header row
        return None

    def _find_end_row(self, all_data, start_row):
        for index, row in all_data.iterrows():
            if index > start_row and row.str.contains("Legends").any():
                # End at the row before the stars row after the table
                return index - 2
        return None

    def _convert_to_datetime(self, df, columns):
        """
        Convert specified columns to datetime format using the given date
        format.
        """
        date_format = "%d/%m/%Y"
        for col in columns:
            df[col] = pd.to_datetime(df[col], format=date_format)
        return df

    def _convert_to_numeric(self, df, columns, fill_value=0):
        """
        Convert specified columns to numeric data type.
        """
        for col in columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(fill_value)
        return df

    def _concatenate_overflowing_rows(self, df):
        for index, row in df.iterrows():
            # Check if there are NaN values in the row
            if row.isnull().any():
                df.at[index - 1, "Transaction Remarks"] += row["Transaction Remarks"]

        df.dropna(inplace=True)

        return df

    def _extract_table_data(self, sheet_name, start_row, end_row):
        df = pd.read_excel(
            self.file_path,
            sheet_name=sheet_name,
            skiprows=start_row,
            nrows=end_row - start_row,
        )

        df = df.drop(df.columns[0], axis=1)

        # these columns don't contain data
        df = df.drop(["S No.", "Cheque Number"], axis=1)

        df = self._concatenate_overflowing_rows(df)

        # Convert to relevant data types
        df = self._convert_to_datetime(df, ["Value Date", "Transaction Date"])

        df["ExtractedInfo"] = df["Transaction Remarks"].apply(
            self._extract_narration_info
        )
        df["RefNo"] = df["ExtractedInfo"].apply(lambda x: x.get("transaction_id"))

        df.rename(
            columns={
                "Value Date": "ValueDate",
                "Transaction Date": "TransactionDate",
                "Transaction Remarks": "Narration",
                "Withdrawal Amount (INR )": "WithdrawalAmt",
                "Deposit Amount (INR )": "DepositAmt",
                "Balance (INR )": "ClosingBalance",
            },
            inplace=True,
        )

        # # Remove rows with all null values or stars
        df = df.dropna(how="all")

        return df
