import h2h

def handler(event, context):
    print("Cleaning h2h page...")

    try: # Calls h2h.py script
        h2h.main() 
        print("h2h.py completed successfully")
        return {
            "statusCode": 200,
            "body": "h2h cleaned and stored in S3 successfully!"
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Cleaning failed: {str(e)}"
        }