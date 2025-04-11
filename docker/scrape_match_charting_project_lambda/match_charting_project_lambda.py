import match_charting_project

def handler(event, context):
    print("Scraping match_charting_project page...")

    try: # calls match_charting_project.py
        batch_num = int(event.get("batch", 0))
        match_charting_project.main(batch_num=batch_num) 
        print("match_charting_project.py completed successfully")
        return {
            "statusCode": 200,
            "body": "Match charting project scraped and stored in S3 successfully!"
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Scraping failed: {str(e)}"
        }