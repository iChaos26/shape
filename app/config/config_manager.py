from types import SimpleNamespace


class ConfigManager:
    def __init__(self, app_settings):
        self.app_settings = app_settings

    @property
    def cassandra_connection(self):
        return SimpleNamespace(**self.app_settings.cassandra_connection)