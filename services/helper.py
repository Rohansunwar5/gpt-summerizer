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
    trigger_frequency = {word: {'count': 0, 'message_ids': []} for word in trigger_words}
    frequency_hourly = [0 for _ in range(24)]
    frequency_weekday = defaultdict(int)
    frequency_user = defaultdict(lambda: {"displayName": None, "username": None, "messageCount": 0})
    links = []

    for message in messages:
        try:
            time = datetime.fromisoformat(message['timestamp_raw'])
            frequency_hourly[time.hour] += 1
            frequency_weekday[time.strftime("%A").lower()] += 1

            username = message.get('username') or "null"
            first_name = message.get('first_name') or ""
            last_name = message.get('last_name') or ""
            display_name = f"{first_name} {last_name}".strip() or username

            text = message.get('text') or ""

            is_important = False
            message_part = {
                'sender': username,
                'message_id': message['message_id'],
                'text': text,
                'timestamp': int(time.timestamp())
            }

            # Count user messages
            frequency_user[username]["displayName"] = display_name
            frequency_user[username]["username"] = username
            frequency_user[username]["messageCount"] += 1

            # Count trigger word frequency
            for word in trigger_words:
                if word in text:
                    is_important = True
                    trigger_frequency[word]['count'] += 1
                    trigger_frequency[word]['message_ids'].append(message_part['message_id'])

            # Extract links
            extracted_links = extract_links(text)
            if extracted_links:
                is_important = True
                links.append({
                    'message_id': message_part['message_id'],
                    'links': extracted_links
                })

        except Exception as e:
            print(e)

    # Convert frequency_user to a list of dicts
    user_frequency = sorted(
        [
            {
                "displayName": data["displayName"],
                "username": data["username"],
                "messageCount": data["messageCount"]
            }
            for data in frequency_user.values()
        ],
        key=lambda x: x["messageCount"],
        reverse=True
    )

    return {
        'trigger_frequency': trigger_frequency,
        'frequency_hourly': frequency_hourly,
        'frequency_weekday': frequency_weekday,
        'user_frequency': user_frequency,
        'links': links,
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

    # Merge frequency_hourly
    merged['frequency_hourly'] = [0] * 24
    for stats in (stats1.get('frequency_hourly', []), stats2.get('frequency_hourly', [])):
        for i, count in enumerate(stats):
            merged['frequency_hourly'][i] += count

    # Merge frequency_weekday
    merged['frequency_weekday'] = defaultdict(int)
    for stats in (stats1.get('frequency_weekday', {}), stats2.get('frequency_weekday', {})):
        for day, count in stats.items():
            merged['frequency_weekday'][day] += count

    # Merge user_frequency
    temp_users = defaultdict(lambda: {"displayName": None, "username": None, "messageCount": 0})
    for stats in (stats1.get('user_frequency', []), stats2.get('user_frequency', [])):
        for user in stats:
            username = user['username']
            temp_users[username]["displayName"] = user["displayName"]
            temp_users[username]["username"] = username
            temp_users[username]["messageCount"] += user["messageCount"]

    merged['user_frequency'] = sorted(
        [
            {
                "displayName": data["displayName"],
                "username": data["username"],
                "messageCount": data["messageCount"]
            }
            for data in temp_users.values()
        ],
        key=lambda x: x["messageCount"],
        reverse=True
    )

    # Merge links
    merged['links'] = []
    for stats in (stats1.get('links', []), stats2.get('links', [])):
        merged['links'].extend(stats)

    return dict(merged)
