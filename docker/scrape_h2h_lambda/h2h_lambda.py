import h2h

def handler(event, context):
    print("Scraping h2h page...")

    try: # calls ranking.py
        h2h.main() 
        print("h2h.py completed successfully")
        return {
            "statusCode": 200,
            "body": "h2h scraped and stored in S3 successfully!"
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Scraping failed: {str(e)}"
        }