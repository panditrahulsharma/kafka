import json
from sseclient import SSEClient as EventSource

url = 'https://stream.wikimedia.org/v2/stream/recentchange'
for event in EventSource(url):
    if event.event == 'message':
        try:
            change = json.loads(event.data)
        except ValueError:
            pass
        else:
            with open('data.json', 'w', encoding='utf-8') as f:
                json.dump(change, f, ensure_ascii=False, indent=4)
                
            print(change)