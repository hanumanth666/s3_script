import boto3
import pandas as pd
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

def convert_bytes_to_gb_tb(size):
    size_gb = size / (1024**3)
    if size_gb < 1024:
        return f"{size_gb:.2f} GB"
    else:
        size_tb = size_gb / 1024
        return f"{size_tb:.2f} TB"

def get_bucket_size(bucket_name):
    s3 = boto3.client('s3')
    size = 0
    continuation_token = None
    total_objects = 0  # Track total objects processed
    print(f"Fetching size for bucket: {bucket_name}")

    while True:
        try:
            if continuation_token:
                response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
                print("Continuation token used")
            else:
                response = s3.list_objects_v2(Bucket=bucket_name)
                print("Initial listing")

            contents = response.get('Contents', [])
            batch_size = len(contents)
            total_objects += batch_size
            print(f"Processed {batch_size} objects in this batch, total objects processed: {total_objects}")

            for obj in contents:
                size += obj['Size']

            if response.get('IsTruncated'):
                continuation_token = response['NextContinuationToken']
            else:
                print("No more objects to process")
                break

        except Exception as e:
            print(f"Error fetching objects for bucket {bucket_name}: {e}")
            break
    
    print(f"Total size fetched for bucket {bucket_name}: {convert_bytes_to_gb_tb(size)}")
    return size

def write_bucket_sizes_to_excel(bucket_sizes, file_name):
    rows = [{'Bucket': bucket_name, 'Size': convert_bytes_to_gb_tb(size)} for bucket_name, size in bucket_sizes.items()]
    df = pd.DataFrame(rows)
    df.to_excel(file_name, index=False)
    print(f"Bucket sizes written to {file_name}")

def main(bucket_names, top_n):
    # Fetch bucket sizes concurrently
    with ThreadPoolExecutor() as executor:
        bucket_futures = {executor.submit(get_bucket_size, bucket_name): bucket_name for bucket_name in bucket_names}

        bucket_sizes = {}
        for future in as_completed(bucket_futures):
            bucket_name = bucket_futures[future]
            try:
                size = future.result()
                bucket_sizes[bucket_name] = size
            except Exception as e:
                print(f"Error fetching size for bucket {bucket_name}: {e}")

    # Sort buckets by size in descending order
    sorted_buckets = sorted(bucket_sizes.items(), key=lambda item: item[1], reverse=True)

    # Write all bucket sizes to Excel
    write_bucket_sizes_to_excel(bucket_sizes, 'all_bucket_sizes.xlsx')

    # Print top N buckets
    print(f"\nTop {top_n} buckets by size:")
    top_buckets = sorted_buckets[:top_n]
    for bucket_name, size in top_buckets:
        print(f"  {bucket_name}: {convert_bytes_to_gb_tb(size)}")

if __name__ == "__main__":
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    all_bucket_names = [bucket['Name'] for bucket in response['Buckets']]

    print("All available buckets:")
    for bucket_name in all_bucket_names:
        print(f"  {bucket_name}")

    top_n = int(input("Enter the number of top buckets to display: "))
    main(all_bucket_names, top_n)
