import subprocess
import datetime

def generate_calendar_reminder(event_name, event_time):
    start_time = event_time.strftime('%Y-%m-%dT%H:%M:%S')
    end_time = (event_time + datetime.timedelta(minutes=15)).strftime('%Y-%m-%dT%H:%M:%S')
    
    # Construct the AppleScript as a single line
    script = f'tell application "Calendar" to tell calendar "Reminders" to make new event at end with properties {{summary:"{event_name}", start date:date "{start_time}", end date:date "{end_time}"}}'
    
    subprocess.run(['osascript', '-e', script])

# Example usage:
event_name = "Test Event"
event_time = datetime.datetime.now()
generate_calendar_reminder(event_name, event_time)
