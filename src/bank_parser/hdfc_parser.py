import pandas as pd
from src.utils import get_logger
import re

log = get_logger(__name__)


class HdfcExcelDataReader:
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
            end_row = self._find_end_row(all_data, (start_row or 0) + 1)

            if start_row is not None and end_row is not None:
                df = self._extract_table_data(sheet_name, start_row, end_row)
                return df
            else:
                raise ValueError("Could not find start and/or end row of the table.")
        except Exception as e:
            raise Exception(f"An error occurred while reading the Excel file: {e}")

    def extract_narration_info(self, narration):
        # Define patterns for different types of transactions
        patterns = {
            "UPI": r"UPI-(.*?)-(.*)-(.*?)-(.*)",
            "NEFT": r"NEFT CR-(.*?)-",
            "POS": r"POS (\d+X{6}\d+)\s(.*)$",
            "CRV POS": r"CRV POS (\d+\*{6}\d+)\s(.*)$",
        }

        extracted_info = {}

        for key, pattern in patterns.items():
            match = re.search(pattern, narration)
            if match:
                if key == "UPI":
                    extracted_info["type"] = key
                    extracted_info["sender"] = match.group(3)
                    extracted_info["message"] = match.group(4)
                elif key == "POS" or key == "CRV POS":
                    extracted_info["type"] = key
                    extracted_info["ID"] = match.group(1)
                    extracted_info["sender_name"] = match.group(2)
                else:
                    extracted_info["type"] = key
                    extracted_info[key] = match.group(1)

        return extracted_info

    def _read_all_data(self, sheet_name):
        return pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)

    def _find_start_row(self, all_data):
        for index, row in all_data.iterrows():
            if row.astype(str).str.contains("Narration").any():
                return index  # Start from the row after the header row
        return None

    def _find_end_row(self, all_data, start_row):
        for index, row in all_data.iterrows():
            if index > start_row and row.str.startswith("*").any():
                # End at the row before the stars row after the table
                return index - 2
        return None

    def _convert_to_datetime(self, df, columns):
        date_format = "%d/%m/%y"
        """
        Convert specified columns to datetime format using the given date
        format.
        """
        for col in columns:
            df[col] = pd.to_datetime(df[col], format=date_format, errors="coerce")
        return df

    def _convert_to_numeric(self, df, columns, fill_value=0):
        """
        Convert specified columns to numeric data type.
        """
        for col in columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(fill_value)
        return df

    def _extract_table_data(self, sheet_name, start_row, end_row):
        df = pd.read_excel(
            self.file_path,
            sheet_name=sheet_name,
            skiprows=start_row,
            nrows=end_row - start_row,
        )

        df = df.drop(df.index[0])

        # Convert to relevant data types

        df = self._convert_to_datetime(df, ["Date", "Value Dt"])
        df = self._convert_to_numeric(
            df, ["Withdrawal Amt.", "Deposit Amt.", "Closing Balance"]
        )

        # Remove rows with all null values or stars
        df = df.dropna(how="all")

        df["ExtractedInfo"] = df["Narration"].apply(self.extract_narration_info)

        df["Bank"] = "HDFC"

        df.rename(
            columns={
                "Date": "ValueDate",
                "Narration": "Narration",
                "Chq./Ref.No.": "RefNo",
                "Value Dt": "TransactionDate",
                "Withdrawal Amt.": "WithdrawalAmt",
                "Deposit Amt.": "DepositAmt",
                "Closing Balance": "ClosingBalance",
            },
            inplace=True,
        )

        return df
