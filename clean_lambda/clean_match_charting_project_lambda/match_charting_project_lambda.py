import match_charting_project

def handler(event, context):
    print("Cleaning match_charting_project page...")

    try: # Calls match_charting_project.py script
        match_charting_project.main() 
        print("match_charting_project.py completed successfully")
        return {
            "statusCode": 200,
            "body": "match_charting_project cleaned and stored in S3 successfully!"
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Cleaning failed: {str(e)}"
        }