# services/telegram_extractor.py
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from collections import Counter
from datetime import datetime, timedelta, timezone
import asyncio
import logging
import time
from services.account_manager import AccountRotationManager

logger = logging.getLogger(__name__)

def run_async(coro):
    """Helper function to run async code in sync context"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

class TelegramMessageExtractor:
    def __init__(self, rotation_manager: AccountRotationManager, messages_limit: int = 100):
        self.rotation_manager = rotation_manager
        self.messages_limit = messages_limit
    
    async def _get_messages_async(self, channel_username: str, account: dict, limit: int = None):
        """Extract messages from a Telegram channel"""
        limit = limit or self.messages_limit
        
        async with TelegramClient(
            StringSession(account['session_string']),
            account['api_id'],
            account['api_hash']
        ) as client:
            channel = await client.get_entity(channel_username)
            messages = []
            user_activity = Counter()
            detailed_users = {}
            
            first_message_timestamp = None
            last_message_timestamp = None
            
            async for msg in client.iter_messages(channel, limit=limit):
                if msg.text:
                    if first_message_timestamp is None:
                        first_message_timestamp = msg.date
                    last_message_timestamp = msg.date
                    
                    sender_name = "Unknown"
                    username = None
                    
                    if msg.sender:
                        if hasattr(msg.sender, 'username') and msg.sender.username:
                            username = msg.sender.username
                            sender_name = f"@{msg.sender.username}"
                        elif hasattr(msg.sender, 'first_name'):
                            sender_name = msg.sender.first_name
                            if hasattr(msg.sender, 'last_name') and msg.sender.last_name:
                                sender_name += f" P{msg.sender.last_name}"
                    
                    user_activity[sender_name] += 1
                    
                    user_id = msg.sender_id if msg.sender_id else 0
                    if sender_name not in detailed_users:
                        detailed_users[sender_name] = {
                            'username': username,
                            'display_name': sender_name,
                            'user_id': user_id,
                            'message_count': 0,
                            'first_message_time': msg.date,
                            'last_message_time': msg.date
                        }
                    
                    detailed_users[sender_name]['message_count'] += 1
                    detailed_users[sender_name]['last_message_time'] = msg.date
                    
                    messages.append({
                        'timestamp': msg.date.strftime('%Y-%m-%d %H:%M:%S'),
                        'timestamp_raw': msg.date.isoformat(),  
                        'text': msg.text,
                        'message_id': msg.id,
                        'sender': sender_name,
                        'sender_id': user_id,
                        'username': username,
                        'content': msg.text,  
                        'author': sender_name  
                    })
            
           
            sorted_users = sorted(
                detailed_users.items(), 
                key=lambda x: x[1]['message_count'], 
                reverse=True
            )
            
            top_50_users = []
            for i, (sender_name, user_info) in enumerate(sorted_users[:50], 1):
                top_50_users.append({
                    'rank': i,
                    'display_name': user_info['display_name'],
                    'username': user_info['username'] if user_info['username'] else 'No Username',
                    'telegram_handle': f"@{user_info['username']}" if user_info['username'] else 'No Username',
                    'message_count': user_info['message_count'],
                    'user_id': user_info['user_id'],
                    'first_seen': user_info['first_message_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    'last_seen': user_info['last_message_time'].strftime('%Y-%m-%d %H:%M:%S')
                })
            
            return {
                'messages': messages,
                'user_activity': dict(user_activity),
                'top_active_users': user_activity.most_common(10),
                'top_50_users': top_50_users,
                'account_used': account['index'],
                'total_messages': len(messages),
                'unique_users_count': len(detailed_users),
                'first_message_timestamp': first_message_timestamp.isoformat() if first_message_timestamp else None,
                'last_message_timestamp': last_message_timestamp.isoformat() if last_message_timestamp else None,
                'channel_info': {
                    'username': channel_username,
                    'title': channel.title if hasattr(channel, 'title') else channel_username,
                    'id': channel.id if hasattr(channel, 'id') else None
                }
            }

    def get_messages(self, channel_username: str, limit: int = None):
        """Get messages with retry logic and account rotation"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                account, account_index = self.rotation_manager.get_next_available_account()
                account['index'] = account_index
                
                logger.info(f"Using account {account_index} for {channel_username}")
                result = run_async(self._get_messages_async(channel_username, account, limit))
                
               
                self.rotation_manager.update_account_usage(account_index)
                
                return result
                
            except FloodWaitError as e:
                logger.warning(f"Rate limit hit: {e}")
                self.rotation_manager.mark_account_rate_limited(account_index, e.seconds)
                retry_count += 1
                time.sleep(2)
                
            except Exception as e:
                error_msg = str(e)
                
                if "not found" in error_msg or "username" in error_msg.lower():
                    logger.error(f"Username not found: {channel_username}")
                    raise Exception(f"Telegram channel '{channel_username}' not found.")
                    
                logger.error(f"Error on attempt {retry_count + 1}: {error_msg}")
                retry_count += 1
                
                if retry_count < max_retries:
                    time.sleep(2)
                else:
                    raise Exception(f"Failed after {max_retries} attempts: {error_msg}")
        
        raise Exception(f"Failed to extract messages after all retries")
    
    def get_messages_since(self, channel_username: str, since: datetime, limit: int = None):
        """Get only messages newer than 'since' timestamp"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                account, account_index = self.rotation_manager.get_next_available_account()
                account['index'] = account_index
                
                logger.info(f"Using account {account_index} for incremental scrape of {channel_username} since {since}")
                
                
                result = run_async(self._get_messages_since_async(
                    channel_username, 
                    account, 
                    since,
                    limit
                ))
                
               
                self.rotation_manager.update_account_usage(account_index)
                
               
                if isinstance(result, dict):
                    logger.info(f"Retrieved {result.get('total_messages', 0)} new messages since {since}")
                else:
                    logger.error(f"Unexpected result type: {type(result)}")
                
                return result
                
            except FloodWaitError as e:
                logger.warning(f"Rate limit hit: {e}")
                self.rotation_manager.mark_account_rate_limited(account_index, e.seconds)
                retry_count += 1
                time.sleep(2)
                
            except Exception as e:
                error_msg = str(e)
                
                if "not found" in error_msg or "username" in error_msg.lower():
                    logger.error(f"Username not found: {channel_username}")
                    raise Exception(f"Telegram channel '{channel_username}' not found.")
                    
                logger.error(f"Error on attempt {retry_count + 1}: {error_msg}")
                retry_count += 1
                
                if retry_count < max_retries:
                    time.sleep(2)
                else:
                    raise Exception(f"Failed after {max_retries} attempts: {error_msg}")
        
        raise Exception(f"Failed to extract messages after all retries")

    async def _get_messages_since_async(self, channel_username, account, since, limit=None):
        """Async method to get messages since a specific timestamp"""
        async with TelegramClient(
            StringSession(account['session_string']),
            account['api_id'],
            account['api_hash']
        ) as client:
            try:
                channel = await client.get_entity(channel_username)
                
                messages = []
                user_activity = Counter()
                detailed_users = {}
                first_message_timestamp = None
                last_message_timestamp = None
                
                if since.tzinfo is None:
                    since = since.replace(tzinfo=timezone.utc)
                
                logger.info(f"Fetching messages newer than {since}")
                

                async for message in client.iter_messages(
                    channel,
                    limit=limit if limit else None,
                    
                ):
                    
                    msg_date = message.date
                    if msg_date.tzinfo is None:
                        msg_date = msg_date.replace(tzinfo=timezone.utc)
                    
                    
                    since_utc = since.astimezone(timezone.utc) if since.tzinfo else since.replace(tzinfo=timezone.utc)
                    
                    if msg_date <= since_utc:
                        logger.info(f"Reached messages older than {since_utc}, stopping")
                        break
                    
                    if message.text:
                        if first_message_timestamp is None:
                            first_message_timestamp = message.date
                        last_message_timestamp = message.date
                        
                        sender_name = "Unknown"
                        username = None
                        
                        if message.sender:
                            if hasattr(message.sender, 'username') and message.sender.username:
                                username = message.sender.username
                                sender_name = f"@{message.sender.username}"
                            elif hasattr(message.sender, 'first_name'):
                                sender_name = message.sender.first_name
                                if hasattr(message.sender, 'last_name') and message.sender.last_name:
                                    sender_name += f" {message.sender.last_name}"
                        
                        user_activity[sender_name] += 1
                        
                        user_id = message.sender_id if message.sender_id else 0
                        if sender_name not in detailed_users:
                            detailed_users[sender_name] = {
                                'username': username,
                                'display_name': sender_name,
                                'user_id': user_id,
                                'message_count': 0,
                                'first_message_time': message.date,
                                'last_message_time': message.date
                            }
                        
                        detailed_users[sender_name]['message_count'] += 1
                        detailed_users[sender_name]['last_message_time'] = message.date
                        
                        message_data = {
                            'message_id': message.id,
                            'text': message.text or '',
                            'sender': sender_name,
                            'sender_id': user_id,
                            'username': username,
                            'timestamp': message.date.strftime('%Y-%m-%d %H:%M:%S'),
                            'timestamp_raw': message.date.isoformat(),
                            'content': message.text, 
                            'author': sender_name  
                        }
                        messages.append(message_data)
                
                messages.sort(key=lambda x: x['timestamp_raw'])
                
                logger.info(f"Found {len(messages)} new messages after {since}")
                

                sorted_users = sorted(
                    detailed_users.items(), 
                    key=lambda x: x[1]['message_count'], 
                    reverse=True
                )
                
                top_50_users = []
                for i, (sender_name, user_info) in enumerate(sorted_users[:50], 1):
                    top_50_users.append({
                        'rank': i,
                        'display_name': user_info['display_name'],
                        'username': user_info['username'] if user_info['username'] else 'No Username',
                        'telegram_handle': f"@{user_info['username']}" if user_info['username'] else 'No Username',
                        'message_count': user_info['message_count'],
                        'user_id': user_info['user_id'],
                        'first_seen': user_info['first_message_time'].strftime('%Y-%m-%d %H:%M:%S'),
                        'last_seen': user_info['last_message_time'].strftime('%Y-%m-%d %H:%M:%S')
                    })
                
                return {
                    'messages': messages,
                    'user_activity': dict(user_activity),
                    'top_active_users': user_activity.most_common(10),
                    'top_50_users': top_50_users,
                    'account_used': account['index'],
                    'total_messages': len(messages),
                    'unique_users_count': len(detailed_users),
                    'first_message_timestamp': first_message_timestamp.isoformat() if first_message_timestamp else None,
                    'last_message_timestamp': last_message_timestamp.isoformat() if last_message_timestamp else None,
                    'channel_info': {
                        'username': channel_username,
                        'title': channel.title if hasattr(channel, 'title') else channel_username,
                        'id': channel.id if hasattr(channel, 'id') else None
                    }
                }
                
            except Exception as e:
                logger.error(f"Error getting messages since {since}: {str(e)}")
                raise