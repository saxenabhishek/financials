import pandas as pd
from src.utils import get_logger
import re

log = get_logger(__name__)


class IciciExcelDataReader:
    invalid_init = False

    def __init__(self, file_paths: list[str]):
        self.file_paths = [
            file_path for file_path in file_paths if "old" not in file_path
        ]

        if len(self.file_paths) == 0:
            self.invalid_init = True

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
        if self.invalid_init:
            raise ValueError("No valid file paths were provided.")
        try:
            transaction_tables = []
            for file_path in self.file_paths:
                all_data = self._read_all_data(file_path)
                start_row = self._find_start_row(all_data)
                end_row = self._find_end_row(all_data, (start_row or 0) + 1)

                if start_row is not None and end_row is not None:
                    log.debug(f"Start row: {start_row}")
                    log.debug(f"End row: {end_row}")
                    transaction_tables.append(
                        self._extract_table_data(file_path, start_row, end_row)
                    )
                    log.debug(
                        f"{len(transaction_tables[-1])} Transactions read from file: {file_path}"
                    )
                else:
                    raise ValueError(
                        "Could not find start and/or end row of the table."
                    )
            return self._combine_dataframes(transaction_tables)
        except Exception as e:
            raise Exception(f"An error occurred while reading the Excel file: {e}")

    def _extract_narration_info(self, narration):
        extracted_info = {}

        try:
            extracted_info = self._extract_upi_info(narration)
        except Exception as e:
            log.debug(f"Error parsing UPI string: {e}")
            extracted_info = self._extract_other_transactions(narration)

        if len(extracted_info) == 0:
            log.debug(f"No pattern matched for narration: {narration}")
            extracted_info["transaction_id"] = narration

        return extracted_info

    def _read_all_data(self, file_path, sheet_name=0):
        return pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    def _find_start_row(self, all_data):
        for index, row in all_data.iterrows():
            if row.astype(str).str.contains("S No.").any():
                return index  # Start from the row after the header row
        return None

    def _find_end_row(self, all_data, start_row):
        for index, row in all_data.iterrows():
            if index > start_row and row.str.contains("Legends").any():
                # End at the row before the stars row after the table
                return index - 1
        return None

    def _convert_to_datetime(self, df, columns):
        """
        Convert specified columns to datetime format using the given date
        format.
        """
        date_format = "%d/%m/%Y"
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

    def _concatenate_overflowing_rows(self, df):
        for index, row in df.iterrows():
            # Check if there are NaN values in the row
            if row.isnull().any():
                df.at[index - 1, "Transaction Remarks"] += row["Transaction Remarks"]

        df.dropna(inplace=True)

        return df

    def _extract_table_data(self, file_path, start_row, end_row, sheet_name=0):
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            skiprows=start_row,
            nrows=end_row - start_row,
        )

        df = df.drop(df.columns[0], axis=1)

        # Remove rows with all null values or stars
        df = df.dropna(how="all")

        # these columns don't contain data
        df = df.drop(["S No.", "Cheque Number"], axis=1)

        df = self._concatenate_overflowing_rows(df)

        # Convert to relevant data types
        df = self._convert_to_datetime(df, ["Value Date", "Transaction Date"])

        return df

    def _combine_dataframes(self, dfs):
        combined_df = pd.concat(dfs)
        combined_df.reset_index(drop=True, inplace=True)
        combined_df.drop_duplicates(inplace=True)

        combined_df["ExtractedInfo"] = combined_df["Transaction Remarks"].apply(
            self._extract_narration_info
        )
        combined_df["_id"] = (
            combined_df["ExtractedInfo"].apply(lambda x: x.get("transaction_id"))
            + combined_df["Withdrawal Amount (INR )"].astype(str)
            + combined_df["Deposit Amount (INR )"].astype(str)
        )

        combined_df["Bank"] = "ICICI"

        combined_df.rename(
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

        return combined_df

    def _extract_upi_info(self, upi_string: str) -> dict:
        """
        Parses an UPI string
        1. "UPI"
        1. upi txn number if all digits or UPI id receiver
        1. "UPI" or transaction Message
        1. If contains bank then receiver bank id otherwise receiver ID
        1. if contains bank then bank id otherwise transaction number
        1. Transaction ID with bank, check for empty strings

        """
        segments = upi_string.split("/")

        # Check for exactly 6 segments
        if len(segments) != 6:
            raise ValueError("Invalid UPI string: Exactly 6 segments required. " + upi_string)

        # Verify that the first segment is "UPI" for sanity check
        if segments[0] != "UPI":
            raise ValueError("Invalid UPI string: First segment must be 'UPI'. " + upi_string)

        # Initialize result dictionary
        result = {}

        # Process each segment based on the specified conditions
        # Element 1: Check if all digits, add as "transaction_id" or as "receiverID"
        result["transaction_id" if segments[1].isdigit() else "beneficiary_details"] = (
            segments[1]
        )

        # Element 2: If not "UPI", add as "message"
        if segments[2] != "UPI":
            result["message"] = segments[2]

        # Element 3: Check for "bank" (case-insensitive) to determine if "receiver bank" or "receiverID"
        if "bank" in segments[3].lower():
            result["beneficiary_bank"] = segments[3]
        else:
            result["beneficiary_details"] = segments[3]

        # Element 4: Check for "bank" (case-insensitive) to determine if "receiver bank" or "txn number"
        if "bank" in segments[4].lower():
            result["beneficiary_bank"] = segments[4]
        else:
            result["transaction_id"] = segments[4]

        # Element 5: Add as "bankTxnId" if not empty
        if segments[5]:
            result["bank_txn_id"] = segments[5]

        return result

    def _extract_other_transactions(self, narration):
        # Define patterns for different types of transactions (and they may expand)
        patterns = {
            "NEFT": r"NEFT-(.*?)-(.*?)-(.*)",
            "Interest": r"(\d+):Int\.Pd:(\d{2}-\d{2}-\d{4}) to (\d{2}-\d{2}-\d{4})",
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
                    extracted_info["sender_bank"] = match.group(2)
                    extracted_info["transaction_id"] = match.group(1)
                    extracted_info["details"] = match.group(3)
                elif key == "Interest":
                    extracted_info["transaction_id"] = match.group(1)
                    extracted_info["start_date"] = match.group(2)
                    extracted_info["end_date"] = match.group(3)

        return extracted_info
