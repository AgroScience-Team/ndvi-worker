class Log:
    def __call__(self, func):
        func._is_log = True  # Добавляем метаинформацию
        return func
