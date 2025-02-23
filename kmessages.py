import itertools
class Kmessage:
    
    id_iter = itertools.count(1)

    def __init__(self, is_selected , message_json):
        self.id = next(Kmessage.id_iter)
        self.is_selected = is_selected
        self.message_json = message_json

