# app.py
from flask import Flask, request, jsonify
from datetime import datetime
import logging
from config import config
from services.account_manager import AccountRotationManager
from services.helper import generate_message_statistics
from services.telegram_extractor import TelegramMessageExtractor
from services.gpt_summerizer import GPTSummarizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

rotation_manager = AccountRotationManager(config.ACCOUNTS_FILE)
extractor = TelegramMessageExtractor(rotation_manager, config.MESSAGES_LIMIT)
summarizer = GPTSummarizer(config.OPENAI_API_KEY)
scraper = TelegramMessageExtractor(rotation_manager=rotation_manager, messages_limit=100)


def calculate_time_difference(messages):
    """Calculate time difference in milliseconds between first and last message"""
    if not messages or len(messages) < 2:
        return 0
    
    try:

        first_time = datetime.fromisoformat(
            messages[0]['timestamp_raw'].replace('Z', '+00:00') if messages[0].get('timestamp_raw') 
            else messages[0]['timestamp'].replace('Z', '+00:00')
        )
        last_time = datetime.fromisoformat(
            messages[-1]['timestamp_raw'].replace('Z', '+00:00') if messages[-1].get('timestamp_raw')
            else messages[-1]['timestamp'].replace('Z', '+00:00')
        )
        

        time_diff = abs((first_time - last_time).total_seconds() * 1000)
        return int(time_diff)
    except Exception as e:
        logger.error(f"Error calculating time difference: {e}")
        return 0
# ============= HEALTH & INFO ENDPOINTS =============

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    accounts, _ = rotation_manager.load_accounts()
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "accounts_configured": len(accounts),
        "storage_type": "JSON File",
        "version": "2.0.0"
    })

@app.route('/supported-languages', methods=['GET'])
def get_supported_languages():
    """Get list of supported languages"""
    return jsonify({
        "success": True,
        "supported_languages": [
            {
                "code": code,
                "english_name": lang["english"],
                "native_name": lang["native"]
            }
            for code, lang in config.SUPPORTED_LANGUAGES.items()
        ],
        "default_language": "english"
    })

# ============= CORE ANALYSIS ENDPOINTS =============

@app.route('/analyze-channel', methods=['POST'])
def analyze_channel():
    """Analyze a Telegram channel"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        channel_username = data.get('channel_username')
        response_language = data.get('language', 'english').lower()
        trigger_words = data.get('triggerWords', ['important', 'urgent', 'announcement', 'update'])

        if not channel_username:
            return jsonify({"error": "channel_username is required"}), 400
        
        if response_language not in config.SUPPORTED_LANGUAGES:
            return jsonify({
                "error": f"Unsupported language: {response_language}"
            }), 400
        
        logger.info(f"Analyzing channel: {channel_username} in {response_language}")
        
        # Get messages data
        messages_data = extractor.get_messages(channel_username)
        
        if not messages_data['messages']:
            return jsonify({"error": "No messages found"}), 404
        
        # Get existing analysis from summarizer
        analysis_result = summarizer.analyze_telegram_group(messages_data, response_language)

        # Generate message statistics using helper function (like /scrape does)
        message_analysis = None
        messages = messages_data['messages']
        
        if messages:
            try:
                logger.info(f"Generating message analysis for {len(messages)} messages with trigger words: {trigger_words}")
                message_analysis = generate_message_statistics(messages, trigger_words)
                logger.info(f"Analysis complete: trigger frequency and other statistics generated")
            except Exception as e:
                logger.error(f"Error generating message analysis: {str(e)}", exc_info=True)
                # Continue without analysis rather than failing the entire request
                message_analysis = None
        
        response_data = {
            "success": True,
            "channel": channel_username,
            "channel_info": messages_data.get('channel_info', {}),
            "analysis": analysis_result['analysis'],
            "statistics": analysis_result['statistics'],
            "top_50_users": analysis_result['top_50_users_list'],
            "response_language": analysis_result['response_language'],
            "timestamps": {
                "first_message": messages_data.get('first_message_timestamp'),
                "last_message": messages_data.get('last_message_timestamp')
            },
            "account_used": messages_data['account_used'],
            "processed_at": datetime.now().isoformat()
        }

        # Add message analysis from helper function (like /scrape does)
        if message_analysis:
            response_data['message_analysis'] = message_analysis
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error in analyze_channel: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

# ============= BOOKMARK SUPPORT ENDPOINTS =============
@app.route('/scrape', methods=['POST'])
def scrape_channel():
    try:
        data = request.json
        channel_name = data.get('channelName')
        limit = data.get('limit', 100)  
        since = data.get('since')  

        # Get triggerWords from request, use default only if not provided
        trigger_words = data.get('triggerWords')
        if trigger_words is None or len(trigger_words) == 0:
            # Use default trigger words only when none are provided
            trigger_words = ['important', 'urgent', 'announcement', 'update']
            logger.info(f"No trigger words provided, using defaults: {trigger_words}")
        else:
            # Use the dynamic trigger words sent from Node.js
            logger.info(f"Using dynamic trigger words from request: {trigger_words}")
        
        if not channel_name:
            return jsonify({
                'success': False,
                'error': 'Channel name is required'
            }), 400
        
        result = None
        
        if since:
            try:
                if isinstance(since, str):
                    since_datetime = datetime.fromisoformat(since.replace('Z', '+00:00'))
                else:
                    since_datetime = since  
                
                logger.info(f"Incremental scrape for {channel_name} since {since_datetime} (timezone: {since_datetime.tzinfo}) with trigger words: {trigger_words}")
                
                result = scraper.get_messages_since(
                    channel_username=channel_name,
                    since=since_datetime,
                    limit=limit
                )
                
                logger.info(f"Incremental scrape result: {len(result.get('messages', []))} messages found")
                
            except ValueError as e:
                logger.error(f"Invalid date format: {since}")
                return jsonify({
                    'success': False,
                    'error': f'Invalid date format: {since}'
                }), 400
        else:
            logger.info(f"Initial scrape for {channel_name}, limit: {limit} with trigger words: {trigger_words}")
            
            result = scraper.get_messages(
                channel_username=channel_name,
                limit=limit
            )
        
        if not result or not isinstance(result, dict):
            logger.error(f"Invalid result from scraper: {type(result)}")
            return jsonify({
                'success': False,
                'error': 'Failed to scrape messages - invalid response from scraper'
            }), 500
        
        messages = result.get('messages', [])
        
        if messages and isinstance(messages, list):
            messages.sort(key=lambda x: x.get('timestamp_raw', x.get('timestamp', '')), reverse=True)
        
        first_msg_timestamp = None
        last_msg_timestamp = None
        
        if messages:
            first_msg_timestamp = messages[0].get('timestamp_raw', messages[0].get('timestamp'))
            last_msg_timestamp = messages[-1].get('timestamp_raw', messages[-1].get('timestamp'))

        # Generate message statistics with dynamic trigger words
        message_analysis = None
        if messages: 
            try: 
                logger.info(f"Generating message analysis for {len(messages)} messages with trigger words: {trigger_words}")
                message_analysis = generate_message_statistics(messages, trigger_words)
                
                # Log how many messages matched the trigger words
                important_count = len(message_analysis.get('important_messages', {}))
                logger.info(f"Analysis complete: {important_count} important messages found using trigger words: {trigger_words}")
                
            except Exception as e:
                logger.error(f"Error generating message analysis: {str(e)}", exc_info=True)
                # Continue without analysis rather than failing the entire request
                message_analysis = None

        response_data = {
            'success': True,
            'messages': messages,
            'channelInfo': result.get('channel_info', {
                'name': channel_name,
                'scraped_at': datetime.now().isoformat()
            }),
            'statistics': { 
                'message_count': len(messages),
                'first_message_timestamp': first_msg_timestamp,  
                'last_message_timestamp': last_msg_timestamp,   
                'time_difference_ms': calculate_time_difference(messages),
                'unique_users_count': result.get('unique_users_count', 0)
            },
            # Include the trigger words used in the response for debugging
            'triggerWordsUsed': trigger_words
        }

        if message_analysis:
            response_data['analysis'] = message_analysis
        
        logger.info(f"Successfully scraped {len(messages)} messages from {channel_name} using trigger words: {trigger_words}")
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Scraping error: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
    
@app.route('/summarize-messages', methods=['POST'])
def summarize_messages():
    """Summarize a collection of messages (for bookmark alerts)"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        messages = data.get('messages', [])
        channel_name = data.get('channelName', 'Unknown Channel')
        response_language = data.get('language', 'english').lower()
        trigger_words = data.get('triggerWords', ['important', 'urgent', 'announcement', 'update'])
        
        if not messages:
            return jsonify({"error": "No messages provided"}), 400
        
        if response_language not in config.SUPPORTED_LANGUAGES:
            return jsonify({
                "error": f"Unsupported language: {response_language}"
            }), 400
        
        logger.info(f"Summarizing {len(messages)} messages for {channel_name}")
        
        # Generate comprehensive AI summary
        summary = summarizer.summarize_combined_messages(
            messages, 
            channel_name, 
            response_language
        )
        
        # Generate static analysis (same as analyze-channel)
        static_analysis = generate_message_statistics(messages, trigger_words)
        
        # Calculate basic statistics
        user_activity = {}
        for msg in messages:
            sender = msg.get('sender', msg.get('username', 'Unknown'))
            user_activity[sender] = user_activity.get(sender, 0) + 1
        
        # Get top users
        top_users = sorted(user_activity.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Calculate timestamps
        first_message_timestamp = None
        last_message_timestamp = None
        if messages:
            # Sort messages by timestamp to find first and last
            sorted_messages = sorted(messages, key=lambda x: x.get('timestamp_raw', x.get('timestamp', '')))
            if sorted_messages:
                first_message_timestamp = sorted_messages[0].get('timestamp_raw', sorted_messages[0].get('timestamp'))
                last_message_timestamp = sorted_messages[-1].get('timestamp_raw', sorted_messages[-1].get('timestamp'))
        
        # Build comprehensive response similar to analyze-channel
        response_data = {
            "success": True,
            "channel": channel_name,
            "channel_info": {
                "name": channel_name,
                "summarized_at": datetime.now().isoformat()
            },
            "analysis": summary,  # Main AI-generated summary
            "statistics": {
                "total_messages": len(messages),
                "unique_users": len(user_activity),
                "top_users": top_users,
                "messages_per_user": len(messages) / len(user_activity) if user_activity else 0
            },
            "response_language": {
                "code": response_language.lower(),
                "english_name": config.SUPPORTED_LANGUAGES[response_language]["english"],
                "native_name": config.SUPPORTED_LANGUAGES[response_language]["native"]
            },
            "timestamps": {
                "first_message": first_message_timestamp,
                "last_message": last_message_timestamp
            },
            "processed_at": datetime.now().isoformat(),
            # Include all static analysis data
            **static_analysis
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error in summarize_messages: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/channel-info/<channel_username>', methods=['GET'])
def get_channel_info(channel_username):
    """Get basic channel information without scraping all messages"""
    try:
        logger.info(f"Getting info for channel: {channel_username}")
        
        messages_data = extractor.get_messages(channel_username, limit=1)
        
        return jsonify({
            "success": True,
            "channel_info": messages_data.get('channel_info', {}),
            "exists": True,
            "checked_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting channel info: {str(e)}")
        return jsonify({
            "success": False,
            "exists": False,
            "error": str(e)
        }), 404

# ============= ACCOUNT MANAGEMENT ENDPOINTS =============

@app.route('/accounts/status', methods=['GET'])
def accounts_status():
    """Get status of all configured accounts"""
    try:
        status = rotation_manager.get_accounts_status()
        
        return jsonify({
            "success": True,
            **status
        })
        
    except Exception as e:
        logger.error(f"Error getting accounts status: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/accounts/reset-limits', methods=['POST'])
def reset_rate_limits():
    """Reset rate limits for all accounts (admin function)"""
    try:
        rotation_manager.reset_rate_limits()
        
        return jsonify({
            "success": True,
            "message": "Rate limits reset for all accounts"
        })
        
    except Exception as e:
        logger.error(f"Error resetting rate limits: {str(e)}")
        return jsonify({"error": str(e)}), 500

# ============= ERROR HANDLERS =============

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint not found",
        "message": str(error)
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Internal server error",
        "message": str(error)
    }), 500

# ============= MAIN ENTRY POINT =============

if __name__ == '__main__':
    if not config.OPENAI_API_KEY:
        logger.error("Missing required environment variable: OPENAI_API_KEY")
        exit(1)
    
    accounts, _ = rotation_manager.load_accounts()
    if not accounts:
        logger.warning("No Telegram accounts configured. Please add accounts to telegram_accounts.json")
    
    logger.info("Starting Telegram Channel Analysis API server...")
    logger.info(f"Version: 2.0.0 - With Bookmark Support")
    logger.info(f"Accounts configured: {len(accounts)}")
    logger.info(f"Supported languages: {', '.join(config.SUPPORTED_LANGUAGES.keys())}")
    logger.info(f"API URL: http://{config.API_HOST}:{config.API_PORT}")
    
    app.run(
        debug=True,
        host=config.API_HOST,
        port=config.API_PORT
    )