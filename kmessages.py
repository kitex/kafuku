import itertools
class Kmessage:
    
    id_iter = itertools.count(1)

    def __init__(self, is_selected , message_json,timestamp_type,timestamp_value):
        self.id = next(Kmessage.id_iter)
        self.is_selected = is_selected
        self.message_json = message_json
        self.timestamp_type = timestamp_type
        self.timestamp_value = timestamp_value

