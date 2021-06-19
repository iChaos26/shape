class GlobalLog:

    __log = None

    @staticmethod
    def get_logger():
        if GlobalLog.__log is None:
            raise Exception('Logger was not set yet...')
        return GlobalLog.__log

    @staticmethod
    def set_logger(logger):
        if GlobalLog.__log is not None:
            raise Exception('Logger was already set...')
        GlobalLog.__log = logger
