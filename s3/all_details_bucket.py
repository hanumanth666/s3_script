import boto3
from collections import defaultdict
import pandas as pd
import math
import json
from botocore.exceptions import ClientError

# Pricing structure for different storage classes in USD per GB per month
STORAGE_CLASS_PRICING = {
    'STANDARD': 0.023,
    'INTELLIGENT_TIERING': 0.023,
    'STANDARD_IA': 0.0125,
    'ONEZONE_IA': 0.01,
    'GLACIER': 0.004,
    'DEEP_ARCHIVE': 0.00099
}

def get_bucket_policy(bucket_name):
    s3 = boto3.client('s3')
    try:
        response = s3.get_bucket_policy(Bucket=bucket_name)
        return json.loads(response['Policy'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
            return {}
        else:
            raise e

def get_lifecycle_policies(bucket_name):
    s3 = boto3.client('s3')
    try:
        response = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        return response['Rules']
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return []
        else:
            raise e

def list_all_prefixes(bucket_name):
    s3 = boto3.client('s3')
    prefixes = set()
    continuation_token = None
    
    while True:
        try:
            if continuation_token:
                response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/', ContinuationToken=continuation_token)
                print("Continuation token used")
            else:
                response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
                print("Starting to list prefixes")

            for prefix in response.get('CommonPrefixes', []):
                prefixes.add(prefix['Prefix'])
            
            if response.get('IsTruncated'):
                continuation_token = response['NextContinuationToken']
                print("Response truncated, continuing with next token")
            else:
                print("No more prefixes to process")
                break

        except Exception as e:
            print(f"Error occurred while listing prefixes: {e}")
            break
    
    return prefixes

def get_objects_data(bucket_name, prefix):
    s3 = boto3.client('s3')
    objects = []
    continuation_token = None
    total_objects = 0  # Track total objects retrieved for debugging

    while True:
        try:
            if continuation_token:
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
                print(f"Continuation token used")
            else:
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                print(f"Starting to list objects for prefix {prefix}")

            batch_size = len(response.get('Contents', []))
            objects.extend(response.get('Contents', []))
            total_objects += batch_size
            print(f"Processed {batch_size} objects in this batch, total objects processed: {total_objects}")

            if response.get('IsTruncated'):
                continuation_token = response['NextContinuationToken']
                print("Response truncated, continuing with next token")
            else:
                print(f"No more objects to process for prefix {prefix}")
                break

        except Exception as e:
            print(f"Error occurred while fetching objects for prefix {prefix}: {e}")
            break

    return objects

def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

def calculate_storage_class_data(objects):
    storage_class_data = defaultdict(int)
    prefix_details = defaultdict(lambda: {'TotalSize': 0, 'StorageClass': defaultdict(int)})
    
    for obj in objects:
        storage_class = obj.get('StorageClass', 'STANDARD')  # Default to STANDARD if not specified
        size = obj['Size']
        storage_class_data[storage_class] += size
        
        prefix_path = '/'.join(obj['Key'].split('/')[:-1]) + '/'
        prefix_details[prefix_path]['TotalSize'] += size
        prefix_details[prefix_path]['StorageClass'][storage_class] += size
    
    return storage_class_data, prefix_details

def calculate_cost(storage_class_data):
    storage_class_cost = {}
    for storage_class, size in storage_class_data.items():
        cost_per_gb = STORAGE_CLASS_PRICING.get(storage_class, 0)
        cost = cost_per_gb * (size / (1024**3))  # Convert size from bytes to GB
        storage_class_cost[storage_class] = round(cost, 2)
    return storage_class_cost

def write_storage_class_to_excel(storage_class_data, writer):
    df = pd.DataFrame([
        {
            'StorageClass': storage_class, 
            'TotalSize (bytes)': size, 
            'TotalSize (GB)': round(size / (1024**3), 2),
            'TotalSize (TB)': round(size / (1024**4), 2)
        } for storage_class, size in storage_class_data.items()
    ])
    df.to_excel(writer, sheet_name='StorageClassData', index=False)

def write_prefix_details_to_excel(prefix_details, lifecycle_policies, bucket_policy, writer):
    rows = []
    for prefix, details in prefix_details.items():
        cost_data = calculate_cost(details['StorageClass'])
        lifecycle = get_lifecycle_for_prefix(prefix, lifecycle_policies)
        lifecycle_str = json.dumps(lifecycle, indent=2)
        bucket_policy_str = json.dumps(bucket_policy, indent=2)
        for storage_class, size in details['StorageClass'].items():
            rows.append({
                'Prefix': prefix,
                'TotalSize (bytes)': details['TotalSize'],
                'TotalSize (GB)': round(details['TotalSize'] / (1024**3), 2),
                'TotalSize (TB)': round(details['TotalSize'] / (1024**4), 2),
                'StorageClass': storage_class,
                'Size (bytes)': size,
                'Size (GB)': round(size / (1024**3), 2),
                'Size (TB)': round(size / (1024**4), 2),
                'Cost': cost_data[storage_class],
                'Lifecycle': lifecycle_str,
                'BucketPolicy': bucket_policy_str
            })
    
    df = pd.DataFrame(rows)
    df.sort_values(by='TotalSize (bytes)', ascending=False, inplace=True)
    df.to_excel(writer, sheet_name='PrefixDetails', index=False)

def get_lifecycle_for_prefix(prefix, lifecycle_policies):
    applicable_policies = []
    for policy in lifecycle_policies:
        if 'Filter' in policy:
            filter = policy['Filter']
            if 'Prefix' in filter and prefix.startswith(filter['Prefix']):
                applicable_policies.append(policy)
            elif 'And' in filter and 'Prefix' in filter['And'] and prefix.startswith(filter['And']['Prefix']):
                applicable_policies.append(policy)
        else:
            # If no specific filter, it applies to the entire bucket
            applicable_policies.append(policy)
    return applicable_policies

def summarize_lifecycle_rules(bucket_name, lifecycle_rules):
    summary = []
    for rule in lifecycle_rules:
        rule_summary = {
            'Bucket': bucket_name,
            'ID': rule.get('ID', 'N/A'),
            'Prefix': rule.get('Filter', {}).get('Prefix', 'N/A'),
            'Status': rule.get('Status', 'N/A'),
            'Transition Days': 'N/A',
            'Storage Class': 'N/A',
            'Expiration Days': 'N/A',
            'Delete Marker': 'No'
        }
        if 'Transitions' in rule:
            transitions = rule['Transitions']
            for transition in transitions:
                rule_summary['Transition Days'] = transition.get('Days', 'N/A')
                rule_summary['Storage Class'] = transition.get('StorageClass', 'N/A')
        if 'Expiration' in rule:
            rule_summary['Expiration Days'] = rule['Expiration'].get('Days', 'N/A')
            if rule['Expiration'].get('ExpiredObjectDeleteMarker', False):
                rule_summary['Delete Marker'] = 'Yes'
        summary.append(rule_summary)
    return summary

def write_lifecycle_summary_to_excel(lifecycle_summary, writer):
    df = pd.DataFrame(lifecycle_summary)
    df.to_excel(writer, sheet_name='LifecycleRules', index=False)

def main(bucket_names, top_n):
    for bucket_name in bucket_names:
        print(f"Starting analysis for bucket: {bucket_name}")
        prefixes = list_all_prefixes(bucket_name)
        print(f"Retrieved {len(prefixes)} prefixes")

        total_storage_class_data = defaultdict(int)
        all_prefix_details = defaultdict(lambda: {'TotalSize': 0, 'StorageClass': defaultdict(int)})
        lifecycle_policies = get_lifecycle_policies(bucket_name)
        lifecycle_summary = summarize_lifecycle_rules(bucket_name, lifecycle_policies)
        bucket_policy = get_bucket_policy(bucket_name)

        for prefix in prefixes:
            print(f"Processing prefix: {prefix}")
            objects = get_objects_data(bucket_name, prefix)
            print(f"Retrieved {len(objects)} objects for prefix {prefix}")

            storage_class_data, prefix_details = calculate_storage_class_data(objects)

            for storage_class, size in storage_class_data.items():
                total_storage_class_data[storage_class] += size

            for prefix, details in prefix_details.items():
                all_prefix_details[prefix]['TotalSize'] += details['TotalSize']
                for storage_class, size in details['StorageClass'].items():
                    all_prefix_details[prefix]['StorageClass'][storage_class] += size

        with pd.ExcelWriter(f'{bucket_name}.xlsx') as writer:  # Dynamic naming based on bucket name
            write_storage_class_to_excel(total_storage_class_data, writer)

            sorted_prefix_details = sorted(all_prefix_details.items(), key=lambda item: item[1]['TotalSize'], reverse=True)
            top_prefix_details = dict(sorted_prefix_details[:top_n])

            # Write the prefix details to Excel
            write_prefix_details_to_excel(top_prefix_details, lifecycle_policies, bucket_policy, writer)

            # Write the lifecycle summary to Excel
            write_lifecycle_summary_to_excel(lifecycle_summary, writer)

if __name__ == "__main__":
    bucket_names = ['Bucket_name']  # Replace with your S3 bucket names
    top_n = 30
    main(bucket_names, top_n)
