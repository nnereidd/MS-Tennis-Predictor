import player_statistics

def handler(event, context):
    print("Scraping player_statistics page...")

    try: # calls player_statistics.py
        batch_num = int(event.get("batch", 0))
        player_statistics.main(batch_num=batch_num) 
        print("player_statistics.py completed successfully")
        return {
            "statusCode": 200,
            "body": "Player Statistics scraped and stored in S3 successfully!"
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Scraping failed: {str(e)}"
        }