import rankings

def handler(event, context):
    print("Cleaning rankings page...")

    try: # Calls rankings.py script
        rankings.main() 
        print("rankings.py completed successfully")
        return {
            "statusCode": 200,
            "body": "rankings cleaned and stored in S3 successfully!"
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Cleaning failed: {str(e)}"
        }