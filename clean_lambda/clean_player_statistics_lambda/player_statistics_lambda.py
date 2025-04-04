import player_statistics

def handler(event, context):
    print("Cleaning player_statistics page...")

    try: # Calls player_statistics.py script
        player_statistics.main() 
        print("player_statistics.py completed successfully")
        return {
            "statusCode": 200,
            "body": "player_statistics cleaned and stored in S3 successfully!"
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Cleaning failed: {str(e)}"
        }