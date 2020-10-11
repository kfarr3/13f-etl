from edgar3 import edgar as ed
from edgar3 import __version__
import os
import datetime
import ntpath
from google.cloud import storage
from distutils import util

print(f"Using Edgar Version: {__version__}", flush=True)

now = datetime.datetime.now()
start_year = int(os.getenv("START_YEAR", now.year))
start_quarter = int(os.getenv("START_QUARTER", (now.month - 1) // 3 + 1))
end_year = int(os.getenv("END_YEAR", now.year))
end_quarter = int(os.getenv("END_QUARTER", (now.month - 1) // 3 + 1))
bucket_name = os.getenv("BUCKET_NAME", "farr-ai-data-lake")
force_download = bool(util.strtobool(os.getenv("FORCE_DOWNLOAD", "False")))

print(f"Downloading 13Fs for {start_year}:Q{start_quarter}-{end_year}:Q{end_quarter} into {bucket_name}", flush=True)

ed_i = ed.edgar_index.edgar_index()

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
        quarter_month = ((quarter - 1) * 3) + 1

        print(f"Processing {year}:Q{quarter}", end="", flush=True)

        listings_df = ed.get_13f_listings(datetime.datetime(year, quarter_month, 1), False)

        skipped = 0
        downloaded = 0

        base_path = f"etl-13f/reports/{year}/{quarter}"
        known_blobs = [blob.name for blob in storage_bucket.list_blobs(prefix=base_path)]

        for index, file_name in listings_df["File Name"].iteritems():

            path_with_name = f"{base_path}/{ntpath.basename(file_name)}"

            if force_download is True or path_with_name not in known_blobs:
                downloaded += 1
                blob = storage_bucket.blob(path_with_name)
                blob.upload_from_string(ed_i.get_filing(file_name))
            else:
                skipped += 1

        print(f" {downloaded} downloaded, {skipped} skipped", flush=True)
