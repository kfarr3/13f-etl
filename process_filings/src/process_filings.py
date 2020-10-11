from edgar3.filing_13f import Filing_13F
from edgar3 import __version__
import os
import datetime
import csv
from google.cloud import storage
from distutils import util
from io import StringIO


def save_filing(fil: Filing_13F, year: int, quarter: int):

    path_with_name = f"etl-13f/processed/reports/{year}/{quarter}/{fil.accession_number}.csv"

    blob = storage_bucket.blob(path_with_name)

    si = StringIO()

    csv_writer = csv.writer(si, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(
        [
            "ManagerName",
            "CIK",
            "Street1",
            "Street2",
            "City",
            "StateOrCountry",
            "ZipCode",
            "AccessionNumber",
            "PeriodOfReport",
            "SignatureDate",
            "CUSIP",
            "SecurityName",
            "TitleOfClass",
            "ValueOfShares",
            "NumberOfShares",
            "TypeOfShares",
        ]
    )
    for holding in fil.holdings:
        csv_writer.writerow(
            [
                fil.manager_name,
                fil.cik,
                fil.street1,
                fil.street2,
                fil.city,
                fil.state_or_country,
                fil.zip_code,
                fil.accession_number,
                fil.period_of_report,
                fil.signature_date,
                holding.cusip,
                holding.name_of_issuer,
                holding.title_of_class,
                holding.value,
                holding.number,
                holding.type,
            ]
        )

    blob.upload_from_string(si.getvalue().strip("\r\n"))


def process_filing(path: str, year: int, quarter: int) -> bool:
    text = storage_bucket.blob(path).download_as_string().decode("utf-8")

    if len(text) == 0:
        print("Zero length")
        log_failed_process(path, year, quarter)
        return True  # allowed failure??
    elif text.startswith("<!DOCTYPE html>"):
        print("Invalid download")
        log_failed_process(path, year, quarter)
        return True

    fil = Filing_13F(text)

    if "13F-NT" in fil.documents:
        return True  # we don't care about these
    elif "13F-NT/A" in fil.documents:
        return True  # don't care about these either
    elif "13F-HR/A" in fil.documents:
        return True
    try:
        if fil.process():
            save_filing(fil, year, quarter)
        else:
            return False
    except Exception as e:
        print(f"Exception on {path}: {e}")
        print(path)
        log_failed_process(path, year, quarter)
        return False

    return True


def log_failed_process(path: str, year: int, quarter: int):
    file_name = path.split("/")[-1]
    new_path = f"etl-13f/failed_reports/{year}Q{quarter}_{file_name}"

    print(f"Failed on {path}, copied to {new_path}")

    storage_bucket.copy_blob(storage_bucket.blob(path), storage_bucket, new_path)


print(f"Using Edgar Version: {__version__}", flush=True)

now = datetime.datetime.now()
start_year = int(os.getenv("START_YEAR", now.year))
start_quarter = int(os.getenv("START_QUARTER", (now.month - 1) // 3 + 1))
end_year = int(os.getenv("END_YEAR", now.year))
end_quarter = int(os.getenv("END_QUARTER", (now.month - 1) // 3 + 1))
bucket_name = os.getenv("BUCKET_NAME", "farr-ai-data-lake")
force_process = bool(util.strtobool(os.getenv("FORCE_PROCESS", "False")))

print(f"Processing 13Fs for {start_year}:Q{start_quarter}-{end_year}:Q{end_quarter} into {bucket_name}", flush=True)

storage_client = storage.Client()
storage_bucket = storage_client.get_bucket(bucket_name)


for year in range(start_year, end_year + 1):

    # if we're starting, the first quarter of the year can be passed in
    if year == start_year:
        quarter_low = start_quarter
    else:
        quarter_low = 1

    # and if we're ending, the last quarter of the year can be passed in
    if year == end_year:
        quarter_high = end_quarter
    else:
        if year == now.year:
            quarter_high = (now.month - 1) // 3 + 1
        else:
            quarter_high = 4

    for quarter in range(quarter_low, quarter_high + 1):

        print(f"Processing {year}:Q{quarter}", end="", flush=True)

        base_path = f"etl-13f/reports/{year}/{quarter}"
        known_blobs = [blob.name for blob in storage_bucket.list_blobs(prefix=base_path)]

        for file in known_blobs:
            process_filing(file, year, quarter)
        print(f" {len(known_blobs)} processed", flush=True)

