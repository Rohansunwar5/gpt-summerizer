import openai
import logging
import tiktoken
from typing import List, Dict, Any, Optional
from datetime import datetime
from collections import defaultdict
from config import config

logger = logging.getLogger(__name__)

class GPTSummarizer:
    def __init__(self, api_key: str = None):
        self.client = openai.OpenAI(api_key=api_key or config.OPENAI_API_KEY)
        self.supported_languages = config.SUPPORTED_LANGUAGES
    
    def analyze_telegram_group(self, messages_data: Dict[str, Any], response_language: str = "english") -> Dict[str, Any]:
        """Analyze Telegram group messages and generate summary"""
        try:
            messages = messages_data.get('messages', [])
            top_users = messages_data.get('top_active_users', [])
            user_activity = messages_data.get('user_activity', {})
            total_messages = messages_data.get('total_messages', 0)
            top_50_users = messages_data.get('top_50_users', [])
            
            message_text = self._format_messages(messages)
            user_summary = self._format_user_summary(top_users, total_messages)
            top_users_detailed = self._format_top_users(top_50_users)
            
            language_info = self.supported_languages.get(
                response_language.lower(), 
                self.supported_languages["english"]
            )
            
            prompt = self._create_analysis_prompt(
                total_messages,
                user_activity,
                user_summary,
                top_users_detailed,
                message_text,
                language_info
            )
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system", 
                        "content": self._get_system_prompt()
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                max_tokens=16000,
                temperature=0.7
            )
            
            return {
                'analysis': response.choices[0].message.content.strip(),
                'statistics': {
                    'total_messages': total_messages,
                    'unique_users': len(user_activity),
                    'top_users': top_users,
                    'messages_per_user': total_messages / len(user_activity) if user_activity else 0
                },
                'top_50_users_list': top_50_users,
                'response_language': {
                    'code': response_language.lower(),
                    'english_name': language_info["english"],
                    'native_name': language_info["native"]
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing group: {str(e)}")
            raise e
    
    def summarize_combined_messages(self, all_messages: List[Dict], channel_name: str, response_language: str = "english") -> str:
        """
        Summarize combined messages from multiple scrapes (for bookmark alerts)
        Optimized for handling 5-10MB of JSON data
        """
        try:
            logger.info(f"Processing {len(all_messages)} messages for channel {channel_name}")
            
            language_info = self.supported_languages.get(
                response_language.lower(),
                self.supported_languages["english"]
            )
            
            # Determine strategy based on message count
            if len(all_messages) < 1000:
                return self._summarize_small_dataset(all_messages, channel_name, language_info)
            elif len(all_messages) < 10000:
                return self._summarize_medium_dataset(all_messages, channel_name, language_info)
            else:
                return self._summarize_large_dataset(all_messages, channel_name, language_info)
            
        except Exception as e:
            logger.error(f"Error summarizing combined messages: {str(e)}")
            raise e
        
    def _count_tokens(self, text: str, model: str = "gpt-4o") -> int:
        """Count tokens for a given text"""
        try:
            encoding = tiktoken.encoding_for_model(model)
            return len(encoding.encode(text))
        except:
            # Fallback: approximate 4 characters per token
            return len(text) // 4
        
    def _summarize_small_dataset(self, messages: List[Dict], channel_name: str, language_info: Dict) -> str:
        """Handle small datasets (< 1000 messages) - Original approach"""
        time_periods = self._group_messages_by_period(messages)
        
        prompt = f"""
        Summarize the activity from channel "{channel_name}" over the past period.
        
        Total messages: {len(messages)}
        Time periods covered: {len(time_periods)}
        
        Messages grouped by time:
        {self._format_time_periods(time_periods)}
        
        Recent messages sample:
        {self._format_messages_sample(messages, max_messages=50)}
        
        Please provide:
        1. Overall activity summary
        2. Key topics and discussions
        3. Important announcements or decisions
        4. Notable user activities
        5. Any concerning patterns or red flags
        
        {"" if language_info['english'].lower() == "english" else f"Provide the summary in {language_info['english']} ({language_info['native']})."}
        """
        
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at summarizing Telegram channel activity for daily digests."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            max_tokens=2000,
            temperature=0.7
        )
        
        return response.choices[0].message.content.strip()
    
    def _summarize_medium_dataset(self, messages: List[Dict], channel_name: str, language_info: Dict) -> str:
        """Handle medium datasets (1000-10000 messages) using two-pass approach"""
        logger.info(f"Using two-pass summarization for {len(messages)} messages")
        
        # First pass: Create daily summaries
        time_periods = self._group_messages_by_period(messages)
        daily_summaries = []
        
        for date, day_messages in sorted(time_periods.items()):
            if len(day_messages) == 0:
                continue
                
            # Extract key information from each day
            user_activity = defaultdict(int)
            topics = []
            
            for msg in day_messages:
                sender = msg.get('sender', 'Unknown')
                user_activity[sender] += 1
                
                # Simple topic extraction (you could make this more sophisticated)
                text = msg.get('text', '')
                if len(text) > 50:  # Consider longer messages as potentially topic-worthy
                    topics.append(text[:200])
            
            # Get top users for this day
            top_users_day = sorted(user_activity.items(), key=lambda x: x[1], reverse=True)[:5]
            
            daily_summary = {
                'date': date,
                'message_count': len(day_messages),
                'unique_users': len(user_activity),
                'top_users': top_users_day,
                'sample_topics': topics[:5],
                'messages_sample': day_messages[:10]  # Keep sample for context
            }
            
            daily_summaries.append(daily_summary)
        
        # Second pass: Synthesize daily summaries
        prompt = self._create_medium_synthesis_prompt(
            channel_name,
            len(messages),
            daily_summaries,
            language_info
        )
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",  # Using mini model for cost efficiency
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at synthesizing daily activity summaries into comprehensive overviews."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            max_tokens=3000,
            temperature=0.7
        )
        
        return response.choices[0].message.content.strip()
    
    def _create_medium_synthesis_prompt(self, channel_name: str, total_messages: int,
                                       daily_summaries: List[Dict], language_info: Dict) -> str:
        """Create synthesis prompt for medium datasets"""
        language_instruction = ""
        if language_info['english'].lower() != "english":
            language_instruction = f"\n\nProvide the summary in {language_info['english']} ({language_info['native']})."
        
        # Format daily summaries
        summaries_text = []
        for summary in daily_summaries[-20:]:  # Last 20 days max to keep prompt size manageable
            day_text = f"""
            Date: {summary['date']}
            Messages: {summary['message_count']} | Users: {summary['unique_users']}
            Top Users: {', '.join([f"{u[0]} ({u[1]})" for u in summary['top_users'][:3]])}
            """
            
            # Add sample topics if available
            if summary['sample_topics']:
                topics_preview = ' | '.join([t[:50] + '...' for t in summary['sample_topics'][:3]])
                day_text += f"Topics: {topics_preview}\n"
            
            summaries_text.append(day_text.strip())
        
        # Get overall statistics
        total_days = len(daily_summaries)
        avg_messages_per_day = total_messages / total_days if total_days > 0 else 0
        
        # Identify most active days
        most_active_days = sorted(daily_summaries, key=lambda x: x['message_count'], reverse=True)[:5]
        
        return f"""
        Synthesize the activity from channel "{channel_name}" based on daily summaries:
        
        OVERALL STATISTICS:
        - Total messages: {total_messages}
        - Period covered: {daily_summaries[0]['date']} to {daily_summaries[-1]['date']}
        - Total days: {total_days}
        - Average messages per day: {avg_messages_per_day:.1f}
        
        MOST ACTIVE DAYS:
        {chr(10).join([f"- {d['date']}: {d['message_count']} messages" for d in most_active_days])}
        
        DAILY ACTIVITY SUMMARIES (Recent):
        {"="*60}
        {chr(10).join(summaries_text)}
        {"="*60}
        
        Based on this data, provide a comprehensive summary covering:
        
        1. **Activity Trends**: Overall engagement patterns, peak periods, growth/decline
        
        2. **Key Topics**: Main discussion themes across the period
        
        3. **Community Dynamics**: Most active users, interaction patterns
        
        4. **Important Events**: Significant announcements or discussions
        
        5. **Actionable Insights**: 3-5 key takeaways or recommendations
        
        Format with clear headers and bullet points.
        {language_instruction}
        """
    
    def _summarize_large_dataset(self, messages: List[Dict], channel_name: str, language_info: Dict) -> str:
        """Handle very large datasets (10000+ messages / 5-10MB) using multi-pass chunking"""
        logger.info(f"Using multi-pass summarization for {len(messages)} messages")
        
        chunks = self._create_smart_chunks(messages, max_chunk_size=2000)
        logger.info(f"Created {len(chunks)} chunks for processing")
        
        chunk_summaries = []
        
        for i, chunk_data in enumerate(chunks):
            logger.info(f"Processing chunk {i+1}/{len(chunks)} with {len(chunk_data['messages'])} messages")
            
            chunk_prompt = f"""
            Summarize this portion of messages from "{channel_name}":
            
            Time period: {chunk_data['start_date']} to {chunk_data['end_date']}
            Messages in chunk: {len(chunk_data['messages'])}
            Unique users: {len(chunk_data['users'])}
            
            TOP USERS IN THIS CHUNK:
            {self._format_chunk_users(chunk_data['users'])}
            
            SAMPLE MESSAGES:
            {self._format_messages_sample(chunk_data['messages'], max_messages=50)}
            
            Provide concise summary covering:
            1. Main topics discussed
            2. Key events or announcements  
            3. Notable user behaviors
            4. Any concerning content
            
            Keep response under 500 words.
            """
            
            try:
                response = self.client.chat.completions.create(
                    model="gpt-4o-mini",  
                    messages=[
                        {"role": "system", "content": "Summarize this message chunk concisely, focusing on key information."},
                        {"role": "user", "content": chunk_prompt}
                    ],
                    max_tokens=800,
                    temperature=0.7
                )
                
                chunk_summaries.append({
                    'chunk_id': i + 1,
                    'period': f"{chunk_data['start_date']} to {chunk_data['end_date']}",
                    'message_count': len(chunk_data['messages']),
                    'summary': response.choices[0].message.content.strip()
                })
                
            except Exception as e:
                logger.error(f"Error processing chunk {i+1}: {str(e)}")
                continue
        
        # Final synthesis
        final_prompt = self._create_synthesis_prompt(
            channel_name, 
            len(messages), 
            chunk_summaries, 
            language_info
        )
        
        # Check token count and use compressed version if needed
        synthesis_tokens = self._count_tokens(final_prompt)
        if synthesis_tokens > 100000:
            logger.warning(f"Synthesis prompt too large ({synthesis_tokens} tokens), using fallback")
            final_prompt = self._create_compressed_synthesis_prompt(
                channel_name, 
                len(messages), 
                chunk_summaries, 
                language_info
            )
        
        logger.info("Creating final synthesis with GPT-4o")
        
        final_response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at synthesizing multiple summaries into comprehensive, actionable insights."
                },
                {"role": "user", "content": final_prompt}
            ],
            max_tokens=4000,
            temperature=0.7
        )
        
        return final_response.choices[0].message.content.strip()
    
    def _create_smart_chunks(self, messages: List[Dict], max_chunk_size: int = 2000) -> List[Dict]:
        """Create intelligent chunks based on time periods and message volume"""
        chunks = []
        
        # Group messages by period first
        periods = self._group_messages_by_period(messages)
        
        current_chunk = {
            'messages': [],
            'users': defaultdict(int),
            'start_date': None,
            'end_date': None
        }
        
        for date in sorted(periods.keys()):
            day_messages = periods[date]
            
            # If adding this day would exceed chunk size, save current chunk
            if current_chunk['messages'] and len(current_chunk['messages']) + len(day_messages) > max_chunk_size:
                chunks.append(current_chunk)
                current_chunk = {
                    'messages': [],
                    'users': defaultdict(int),
                    'start_date': None,
                    'end_date': None
                }
            
            # Add messages to current chunk
            current_chunk['messages'].extend(day_messages)
            if not current_chunk['start_date']:
                current_chunk['start_date'] = date
            current_chunk['end_date'] = date
            
            # Track users in chunk
            for msg in day_messages:
                sender = msg.get('sender', 'Unknown')
                current_chunk['users'][sender] += 1
        
        # Add final chunk
        if current_chunk['messages']:
            chunks.append(current_chunk)
        
        return chunks

    def _smart_sample_messages(self, messages: List[Dict], target_size: int = 500) -> List[Dict]:
        """Intelligently sample messages to get representative subset"""
        if len(messages) <= target_size:
            return messages
        
        # 40% recent messages, 60% distributed across history
        recent_size = int(target_size * 0.4)
        historical_size = target_size - recent_size
        
        # Get recent messages
        recent_messages = messages[-recent_size:] if len(messages) > recent_size else messages
        
        # Sample historical messages evenly
        if len(messages) > recent_size:
            historical_messages = messages[:-recent_size]
            step = max(1, len(historical_messages) // historical_size)
            sampled_historical = historical_messages[::step][:historical_size]
            return sampled_historical + recent_messages
        
        return recent_messages
    
    def _format_messages_sample(self, messages: List[Dict], max_messages: int = 50) -> str:
        """Format a sample of messages for the prompt"""
        sample = messages[:max_messages] if len(messages) > max_messages else messages
        
        formatted = []
        for msg in sample:
            timestamp = msg.get('timestamp', 'N/A')
            sender = msg.get('sender', 'Unknown')
            text = msg.get('text', '')[:200]  # Limit text length
            formatted.append(f"[{timestamp}] {sender}: {text}")
        
        if len(messages) > max_messages:
            formatted.append(f"\n... and {len(messages) - max_messages} more messages ...")
        
        return "\n".join(formatted)
    
    def _format_message_distribution(self, time_periods: Dict) -> str:
        """Format message distribution statistics"""
        sorted_periods = sorted(time_periods.items())
        
        # Show last 20 days or all if less
        recent_periods = sorted_periods[-20:]
        
        distribution = []
        for date, msgs in recent_periods:
            bar_length = min(50, len(msgs) // 10)  # Scale bar
            bar = "█" * bar_length
            distribution.append(f"{date}: {bar} ({len(msgs)} msgs)")
        
        return "\n".join(distribution)
    
    def _format_detailed_periods(self, periods: Dict, max_per_period: int = 10) -> str:
        """Format detailed messages by period"""
        formatted = []
        
        for date, messages in sorted(periods.items())[-10:]:  # Last 10 periods
            formatted.append(f"\n=== {date} ({len(messages)} messages) ===")
            
            # Sample messages from this period
            for msg in messages[:max_per_period]:
                sender = msg.get('sender', 'Unknown')
                text = msg.get('text', '')[:150]
                formatted.append(f"{sender}: {text}")
            
            if len(messages) > max_per_period:
                formatted.append(f"... {len(messages) - max_per_period} more messages ...")
        
        return "\n".join(formatted)
    
    def _format_chunk_users(self, users: Dict[str, int]) -> str:
        """Format top users in a chunk"""
        sorted_users = sorted(users.items(), key=lambda x: x[1], reverse=True)[:10]
        return "\n".join([f"- {user}: {count} messages" for user, count in sorted_users])
    
    def _create_synthesis_prompt(self, channel_name: str, total_messages: int, 
                                 chunk_summaries: List[Dict], language_info: Dict) -> str:
        """Create the final synthesis prompt"""
        language_instruction = ""
        if language_info['english'].lower() != "english":
            language_instruction = f"\n\nIMPORTANT: Provide the entire summary in {language_info['english']} ({language_info['native']})."
        
        summaries_text = "\n\n".join([
            f"**Chunk {cs['chunk_id']} ({cs['period']}, {cs['message_count']} messages):**\n{cs['summary']}"
            for cs in chunk_summaries
        ])
        
        return f"""
        Synthesize these summaries from channel "{channel_name}" into a comprehensive overview:
        
        OVERALL STATISTICS:
        - Total messages analyzed: {total_messages}
        - Number of time chunks: {len(chunk_summaries)}
        - Coverage period: {chunk_summaries[0]['period'].split(' to ')[0]} to {chunk_summaries[-1]['period'].split(' to ')[1]}
        
        CHUNK SUMMARIES:
        {"="*60}
        {summaries_text}
        {"="*60}
        
        Create a unified, comprehensive summary that:
        
        1. **Activity Overview**: Overall trends, peak activity periods, engagement patterns
        
        2. **Key Topics & Themes**: Identify and elaborate on 5-7 main discussion topics across all chunks
        
        3. **Important Events**: Timeline of significant announcements, decisions, or events
        
        4. **User Dynamics**: Most influential users, community behaviors, interaction patterns
        
        5. **Red Flags**: Any concerning patterns, suspicious activities, or content requiring attention
        
        6. **Actionable Insights**: Specific recommendations based on the analysis
        
        7. **Executive Summary**: 3-5 bullet points with the most critical findings
        
        Format the response clearly with headers and bullet points for easy scanning.
        {language_instruction}
        """
    
    def _create_compressed_synthesis_prompt(self, channel_name: str, total_messages: int,
                                           chunk_summaries: List[Dict], language_info: Dict) -> str:
        """Create a compressed synthesis prompt if the full one is too large"""
        language_instruction = ""
        if language_info['english'].lower() != "english":
            language_instruction = f"\n\nProvide summary in {language_info['english']}."
        
        # Take only the first 200 characters of each summary
        compressed_summaries = []
        for cs in chunk_summaries:
            compressed = cs['summary'][:200] + "..."
            compressed_summaries.append(f"Period {cs['period']}: {compressed}")
        
        return f"""
        Synthesize activity from "{channel_name}" ({total_messages} total messages):
        
        Quick summaries from {len(chunk_summaries)} time periods:
        {chr(10).join(compressed_summaries)}
        
        Provide concise overview covering:
        1. Main activity trends
        2. Top 5 discussion topics
        3. Key events
        4. Any red flags
        5. Top 3 actionable insights
        {language_instruction}
        """
    
    def _format_messages(self, messages: List[Dict]) -> str:
        """Format messages for prompt"""
        return "\n\n".join([
            f"[{msg['timestamp']}] {msg['sender']}: {msg['text'][:300]}"
            for msg in messages[:100]  
        ])
    
    def _format_user_summary(self, top_users: List, total_messages: int) -> str:
        """Format user activity summary"""
        return "\n".join([
            f"- {user}: {count} messages ({count/total_messages:.1%} of total)"
            for user, count in top_users
        ])
    
    def _format_top_users(self, top_50_users: List[Dict]) -> str:
        """Format top 50 users list"""
        return "\n".join([
            f"{user['rank']}. {user['display_name']} ({user['telegram_handle']}) - {user['message_count']} messages"
            for user in top_50_users
        ])
    
    def _create_analysis_prompt(self, total_messages, user_activity, user_summary, 
                                top_users_detailed, message_text, language_info):
        """Create the analysis prompt"""
        language_instruction = ""
        if language_info["english"].lower() != "english":
            language_instruction = f"\n\nIMPORTANT: Provide the entire analysis in {language_info['english']} ({language_info['native']})."
        
        return f"""
        Analyze this Telegram channel based on the last {total_messages} messages:
        
        TOTAL MESSAGES ANALYZED: {total_messages}
        TOTAL UNIQUE USERS: {len(user_activity)}
        
        TOP ACTIVE USERS:
        {user_summary}
        
        TOP 50 USERS WITH TELEGRAM HANDLES:
        {top_users_detailed}
        
        RECENT MESSAGES:
        {message_text}
        
        Please provide a comprehensive analysis covering:
        1. Channel Overview (detailed summary)
        2. Most active users and their messages
        3. Alias Pivoting (Actor Enumeration)
        4. Textual Pattern Mining
        5. Human Trafficking / Adult Scam Connections (if any)
        6. Cryptocurrency Indicators (Hidden) if any
        7. User-to-Alias Relationship Map
        8. Key Insights
        {language_instruction}
        """
    
    def _get_system_prompt(self):
        """Get system prompt for GPT"""
        return "You are an expert analyst specializing in Telegram channel analysis. You can communicate fluently in multiple languages and provide detailed analysis in the requested language."
    
    def _group_messages_by_period(self, messages: List[Dict]) -> Dict:
        """Group messages by time period for better summarization"""
        periods = defaultdict(list)
        for msg in messages:
            try:
                # Handle both timestamp formats
                timestamp_str = msg.get('timestamp_raw', msg.get('timestamp', ''))
                
                # Clean up timestamp string
                if timestamp_str:
                    # Remove 'Z' and add proper timezone
                    if timestamp_str.endswith('Z'):
                        timestamp_str = timestamp_str[:-1] + '+00:00'
                    
                    timestamp = datetime.fromisoformat(timestamp_str)
                    period_key = timestamp.strftime('%Y-%m-%d')
                    periods[period_key].append(msg)
            except Exception as e:
                logger.warning(f"Failed to parse timestamp: {timestamp_str}, error: {e}")
                continue
        
        return dict(periods)
    
    def _format_time_periods(self, periods: Dict) -> str:
        """Format time periods for prompt"""
        formatted = []
        for date, messages in sorted(periods.items()):
            formatted.append(f"{date}: {len(messages)} messages")
        return "\n".join(formatted)