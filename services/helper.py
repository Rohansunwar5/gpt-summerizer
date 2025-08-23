import re
from datetime import datetime
from collections import defaultdict
def extract_links(text):
    """
    Extracts all links from a given text.
    Returns a list of URLs or an empty list if none found.
    """
    # Regex pattern for detecting URLs (http, https, t.me, etc.)
    url_pattern = r'(https?://[^\s]+|www\.[^\s]+)'
    links = re.findall(url_pattern, text)
    return links


def generate_message_statistics(messages, trigger_words):
        # example trigger_words = ['words', 'to', 'listen', 'to']
        trigger_frequency = {word: {'count': 0, 'message_ids': []} for word in trigger_words}
        frequency_hourly = [0 for i in range(24)]
        frequency_weekday = defaultdict(int)
        frequency_user = defaultdict(int)
        links = []
        message_with_references = []
        important_messages = {}

        for message in messages:
            try:
                time = datetime.fromisoformat(message['timestamp_raw'])
                frequency_hourly[time.hour] += 1
                frequency_weekday[time.strftime("%A").lower()] += 1

                sender = sender = message.get('username') or "null"
                text = message.get('text') or ""

                is_important = False
                message_part = {
                    'sender': sender,
                    'message_id': message['message_id'],
                    'text': text,
                    'timestamp': int(time.timestamp())
                }

                frequency_user[sender] += 1
                
                for word in trigger_words:
                    if(word in text):
                        is_important = True
                        trigger_frequency[word]['count'] += 1
                        trigger_frequency[word]['message_ids'].append(message_part['message_id'])
                
                extracted_links = extract_links(text)
                if(extracted_links):
                    is_important = True
                    links.append({
                        'message_id': message_part['message_id'],
                        'links': extracted_links
                    })

                if '@' in text:
                    is_important = True
                    message_with_references.append(message_part['message_id'])
                
                if(is_important):
                    important_messages[message_part['message_id']] = message_part
                
            except Exception as e:
                print(e)
        
        return {
            'trigger_frequency': trigger_frequency,
            'frequency_hourly': frequency_hourly,
            'frequency_weekday': frequency_weekday,
            'frequency_user': frequency_user,
            'links': links,
            'message_with_references': message_with_references,
            'important_messages': important_messages
        }

def merge_message_statistics(stats1, stats2):
    merged = defaultdict(lambda: None)

    # Merge trigger_frequency
    merged['trigger_frequency'] = {}
    for stats in (stats1.get('trigger_frequency', {}), stats2.get('trigger_frequency', {})):
        for word, data in stats.items():
            if word not in merged['trigger_frequency']:
                merged['trigger_frequency'][word] = {'count': 0, 'message_ids': []}
            merged['trigger_frequency'][word]['count'] += data.get('count', 0)
            merged['trigger_frequency'][word]['message_ids'].extend(data.get('message_ids', []))

    # Merge frequency_hourly (list of 24 ints)
    merged['frequency_hourly'] = [0] * 24
    for stats in (stats1.get('frequency_hourly', []), stats2.get('frequency_hourly', [])):
        for i, count in enumerate(stats):
            merged['frequency_hourly'][i] += count

    # Merge frequency_weekday
    merged['frequency_weekday'] = defaultdict(int)
    for stats in (stats1.get('frequency_weekday', {}), stats2.get('frequency_weekday', {})):
        for day, count in stats.items():
            merged['frequency_weekday'][day] += count

    # Merge frequency_user
    merged['frequency_user'] = defaultdict(int)
    for stats in (stats1.get('frequency_user', {}), stats2.get('frequency_user', {})):
        for user, count in stats.items():
            merged['frequency_user'][user] += count

    # Merge links
    merged['links'] = []
    for stats in (stats1.get('links', []), stats2.get('links', [])):
        merged['links'].extend(stats)

    # Merge message_with_references
    merged['message_with_references'] = list(
        set(stats1.get('message_with_references', []) + stats2.get('message_with_references', []))
    )

    # Merge important_messsages
    merged['important_messages'] = {}
    merged['important_messages'].update(stats1.get('important_messages', {}))
    merged['important_messages'].update(stats2.get('important_messages', {}))

    return dict(merged)