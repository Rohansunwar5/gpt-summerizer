import re
import datetime
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


def generate_message_statistics(messages):
        trigger_words = ['words', 'to', 'listen', 'to']  #fetch it from the trigger maybe?
        trigger_frequency = {word: 0 for word in trigger_words}
        frequency_hourly = [0 for i in range(24)]
        frequency_weekday = defaultdict(int)
        frequency_user = {}
        links = []
        message_with_references = 0
        

        for message in messages:
            try:
                time = datetime.fromisoformat(message['timestamp_raw'])
                frequency_hourly[time.hour] += 1
                frequency_hourly[time.strftime("%A")] += 1

                if(message['username'] not in frequency_user):
                    frequency_user[message['username']] = 1
                else:
                    frequency_user[message['username']] += 1
                
                for word in trigger_words:
                    if(word in message):
                        trigger_frequency[word] += 1
                
                links.extend(extract_links(message['content']))

                frequency_weekday[time.strftime("%A").lower()] += 1
                
                if '@' in message_with_references:
                    message_with_references += 1
                
            except Exception as e:
                print(e)
        
        return {
            'trigger_frequency': trigger_frequency,
            'frequency_hourly': frequency_hourly,
            'frequency_weekday': frequency_weekday,
            'frequency_user': frequency_user,
            'links': links,
            'message_with_references': message_with_references
        }