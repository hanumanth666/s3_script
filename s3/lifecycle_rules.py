import boto3
import pandas as pd

def get_bucket_lifecycle_configuration(s3_client, bucket_name):
    try:
        response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        return response.get('Rules', [])
    except s3_client.exceptions.NoSuchLifecycleConfiguration:
        print(f"No lifecycle configuration for bucket: {bucket_name}")
        return []
    except Exception as e:
        print(f"Error fetching lifecycle configuration for bucket {bucket_name}: {e}")
        return []

def parse_lifecycle_rule(rule):
    transitions = rule.get('Transitions', [])
    expiration = rule.get('Expiration', {})
    delete_marker = rule.get('NoncurrentVersionExpiration', {})

    transition_details = []
    for transition in transitions:
        transition_details.append({
            'Transition Days': transition.get('Days', 'N/A'),
            'Storage Class': transition.get('StorageClass', 'N/A')
        })

    expiration_days = expiration.get('Days', 'N/A')
    delete_marker_days = delete_marker.get('NoncurrentDays', 'N/A')

    return transition_details, expiration_days, delete_marker_days

def write_lifecycle_details_to_excel(bucket_details, file_name):
    rows = []

    for bucket_name, rules in bucket_details.items():
        for rule in rules:
            transition_details, expiration_days, delete_marker_days = parse_lifecycle_rule(rule)
            if not transition_details:
                rows.append({
                    'Bucket': bucket_name,
                    'ID': rule.get('ID', 'N/A'),
                    'Prefix': rule.get('Filter', {}).get('Prefix', 'N/A'),
                    'Status': rule.get('Status', 'N/A'),
                    'Transition Days': 'N/A',
                    'Storage Class': 'N/A',
                    'Expiration Days': expiration_days,
                    'Delete Marker': delete_marker_days
                })
            else:
                for transition in transition_details:
                    rows.append({
                        'Bucket': bucket_name,
                        'ID': rule.get('ID', 'N/A'),
                        'Prefix': rule.get('Filter', {}).get('Prefix', 'N/A'),
                        'Status': rule.get('Status', 'N/A'),
                        'Transition Days': transition['Transition Days'],
                        'Storage Class': transition['Storage Class'],
                        'Expiration Days': expiration_days,
                        'Delete Marker': delete_marker_days
                    })

    df = pd.DataFrame(rows)
    df.to_excel(file_name, index=False)

    print(f"Lifecycle details for specified buckets written to {file_name}")

def main():
    s3_client = boto3.client('s3')
    
    # Define the bucket names directly in the script
    bucket_names = [
        'bucket_name' # Replace with actual bucket names
        
    ]
    
    bucket_details = {}
    for bucket_name in bucket_names:
        print(f"Gathering lifecycle details for bucket: {bucket_name}")
        rules = get_bucket_lifecycle_configuration(s3_client, bucket_name)
        bucket_details[bucket_name] = rules
    
    write_lifecycle_details_to_excel(bucket_details, 's3_lifecycle_details.xlsx')

if __name__ == "__main__":
    main()

