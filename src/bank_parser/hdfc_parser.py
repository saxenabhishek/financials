import pandas as pd
from src.utils import get_logger
import re
import hashlib

log = get_logger(__name__)


# Function to generate unique identifier
def generate_unique_id(row):
    # Concatenate values from all columns into a single string
    row_string = "".join(str(value) for value in row)
    print(row_string)
    log.debug(f"Genrating hash for: {row['Narration']}")
    # Generate hash value using SHA-256 hash function
    hash_value = hashlib.sha256(row_string.encode()).hexdigest()
    return hash_value


class HdfcExcelDataReader:
    def __init__(self, file_paths: list[str]):
        if len(file_paths) == 0:
            raise ValueError("No file paths provided.")
        self.file_paths = [
            file_path for file_path in file_paths if "old" not in file_path
        ]

    def read_data(self) -> pd.DataFrame:
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

    def _read_all_data(self, file_path, sheet_name=0):
        return pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    def _find_start_row(self, all_data):
        for index, row in all_data.iterrows():
            if row.astype(str).str.contains("Narration").any():
                return index  # Start from the row after the header row
        return None

    def _find_end_row(self, all_data, start_row):
        for index, row in all_data.iterrows():
            if index > start_row and row.str.startswith("*").any():
                # End at the row before the stars row after the table
                return index - 1
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

    def _extract_table_data(self, file_path, start_row, end_row, sheet_name=0):
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            skiprows=start_row,
            nrows=end_row - start_row,
        )

        df = df.drop(df.index[0])

        # Remove rows with all null values or stars
        df = df.dropna(how="all")

        # Convert to relevant data types
        df = self._convert_to_datetime(df, ["Date", "Value Dt"])
        df = self._convert_to_numeric(
            df, ["Withdrawal Amt.", "Deposit Amt.", "Closing Balance"]
        )
        return df

    def _combine_dataframes(self, dfs):
        combined_df = pd.concat(dfs)
        combined_df.reset_index(drop=True, inplace=True)
        combined_df.drop_duplicates(inplace=True)

        empty_id_rows = combined_df["Chq./Ref.No."] == "0" * 15
        combined_df.loc[empty_id_rows, "Chq./Ref.No."] = combined_df[
            empty_id_rows
        ].apply(generate_unique_id, axis=1)

        combined_df["_id"] = (
            combined_df["Chq./Ref.No."]
            + combined_df["Withdrawal Amt."].astype(str)
            + combined_df["Deposit Amt."].astype(str)
        )

        combined_df.drop("Chq./Ref.No.", axis=1, inplace=True)

        combined_df["ExtractedInfo"] = combined_df["Narration"].apply(
            self.extract_narration_info
        )

        combined_df["Bank"] = "HDFC"

        combined_df.rename(
            columns={
                "Date": "ValueDate",
                "Value Dt": "TransactionDate",
                "Withdrawal Amt.": "WithdrawalAmt",
                "Deposit Amt.": "DepositAmt",
                "Closing Balance": "ClosingBalance",
            },
            inplace=True,
        )

        return combined_df
