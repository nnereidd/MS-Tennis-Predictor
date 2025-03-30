import rankings  

def lambda_handler(event, context):
    print("Scraping rankings page...")

    try: # calls ranking.py
        rankings.main() 
        print("Rankings.py completed successfully")
        return {
            "statusCode": 200,
            "body": "Rankings scraped and stored in S3 successfully!"
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Scraping failed: {str(e)}"
        }
