class LogModel():
    def __init__(self, event_name, record_count, created_at):
        self.event_name = event_name
        self.record_count = record_count
        self.created_at = created_at

    def __str__(self):
        return f"Event Name: {self.event_name}, Record Count: {self.record_count}, Created at: {self.created_at}"