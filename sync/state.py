class StateManager:
    def load(self, path):
        try:
            with open(path) as f:
                return f.read().strip()
        except FileNotFoundError:
            return "1900-01-01 00:00:00"

    def save(self, path, value):
        with open(path, "w") as f:
            f.write(value)
