import logging
import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Настройка путей
WATCH_PATH = "."
PROCESSED_DIR = os.path.join(WATCH_PATH, "Файлы обработанных осциллограмм")
LOG_DIR = os.path.join(WATCH_PATH, "logs")
LOG_FILE = os.path.join(LOG_DIR, "log.txt")

# Убедимся, что необходимые папки существуют
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class DOFileHandler(FileSystemEventHandler):
    def __init__(self, watch_path, processed_dir, delay=1):
        super().__init__()
        self.watch_path = os.path.abspath(watch_path)  # Абсолютный путь наблюдаемой папки
        self.processed_dir = os.path.abspath(processed_dir)  # Абсолютный путь папки с результатами
        self.delay = delay  # Задержка перед обработкой
        self.processed_files = set()  # Множество для хранения обработанных файлов

    def handle_file(self, file_path):
        """Обрабатывает файл, если он с расширением .DO и находится в наблюдаемой папке."""
        abs_path = os.path.abspath(file_path)  # Преобразуем путь в абсолютный
        if not abs_path.startswith(self.watch_path):  # Игнорируем файлы за пределами наблюдаемой папки
            return

        # Если файл уже был обработан, пропускаем его
        if abs_path in self.processed_files:
            logger.info(f"Файл {file_path} уже был обработан в этой сессии, пропускаем.")
            return

        # Если файл уже в папке processed_files, пропускаем его
        if abs_path.startswith(self.processed_dir):
            logger.info(f"Файл {file_path} уже был перемещен в папку processed_files, пропускаем.")
            return

        # Отметить файл как обработанный
        self.processed_files.add(abs_path)

        logger.info(f"Обнаружен файл с расширением .DO: {file_path}")
        time.sleep(self.delay)  # Задержка перед обработкой
        self.process_file(file_path)

    def on_created(self, event):
        """Обработка события создания файла."""
        if not event.is_directory and event.src_path.endswith(".DO"):
            self.handle_file(event.src_path)

    def on_modified(self, event):
        """Обработка события изменения файла."""
        if not event.is_directory and event.src_path.endswith(".DO"):
            self.handle_file(event.src_path)

    def on_moved(self, event):
        """Обработка события перемещения или переименования файла."""
        if not event.is_directory and event.dest_path.endswith(".DO"):
            self.handle_file(event.dest_path)

    @staticmethod
    def change_extension(file_path, new_ext):
        """Меняет расширение файла."""
        return f"{os.path.splitext(file_path)[0]}.{new_ext}"

    @staticmethod
    def is_file_ready(file_path):
        """Проверяет, готов ли файл для чтения (не занят другими процессами)."""
        try:
            with open(file_path, 'rb'):
                return True
        except (IOError, OSError):
            return False

    @staticmethod
    def contains_keywords(file_path, keywords):
        """Проверяет, содержит ли файл указанные ключевые слова в первой строке."""
        try:
            with open(file_path, 'r', encoding='windows-1252', errors='ignore') as file:
                first_line = file.readline()
                return any(keyword in first_line for keyword in keywords)
        except Exception as e:
            logger.error(f"Ошибка чтения файла {file_path}: {e}")
            return False

    def process_file(self, file_path):
        """Обрабатывает файл с расширением .DO."""
        logger.info(f"Начата обработка файла: {file_path}")

        # Дожидаемся, пока файл станет доступен
        while not self.is_file_ready(file_path):
            time.sleep(0.5)

        # Переименовываем в .txt
        txt_file_path = self.change_extension(file_path, 'txt')
        os.rename(file_path, txt_file_path)
        logger.info(f"Файл переименован в {txt_file_path}")

        # Проверяем ключевые слова в файле
        keywords = ['4000', '800', '100']
        if self.contains_keywords(txt_file_path, keywords):
            new_file_name = f"{os.path.splitext(file_path)[0]}ЭлИзнос.DO"
        else:
            new_file_name = f"{os.path.splitext(file_path)[0]}ПКС.DO"

        # Перемещаем обработанный файл в папку processed_files
        final_file_path = os.path.join(self.processed_dir, os.path.basename(new_file_name))
        os.rename(txt_file_path, final_file_path)
        logger.info(f"Обработка завершена. Файл перемещен в {final_file_path}")

        # Добавляем задержку после перемещения файла
        time.sleep(self.delay)  # Добавлена задержка

        # Обновляем список обработанных файлов
        self.processed_files.add(final_file_path)


if __name__ == "__main__":
    # Настройка и запуск наблюдателя
    event_handler = DOFileHandler(WATCH_PATH, PROCESSED_DIR, delay=1)  # Задержка в 1 секунду
    observer = Observer()
    observer.schedule(event_handler, WATCH_PATH, recursive=False)
    observer.start()

    logger.info(f"Наблюдение за папкой: {WATCH_PATH}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Остановка наблюдателя.")
        observer.stop()

    observer.join()
