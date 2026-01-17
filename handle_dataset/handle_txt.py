import csv

# Настройки имен файлов
input_file = 'dataset.txt'
output_file = 'dataset.csv'


def process_file():
    # Открываем исходный файл на чтение и целевой на запись
    with open('../src/app/ml/' + input_file, 'r', encoding='utf-8') as f_in, \
            open(output_file, 'w', encoding='utf-8', newline='') as f_out:

        # Создаем writer для записи CSV (он сам обработает кавычки и запятые в тексте)
        writer = csv.writer(f_out)

        # Пишем заголовок CSV
        writer.writerow(['is_destructive', 'text'])

        count = 0
        for line in f_in:
            line = line.strip()
            if not line:
                continue  # Пропускаем пустые строки

            # 1. Отделяем метки от текста
            # split(' ', 1) делит строку по ПЕРВОМУ пробелу на 2 части
            parts = line.split(' ', 1)

            labels_part = parts[0]
            # Если текст есть, берем его, иначе пустая строка
            text_part = parts[1] if len(parts) > 1 else ""

            # 2. Обрабатываем метки (их может быть несколько через запятую)
            labels = labels_part.split(',')

            is_destructive = 0

            # Логика: если ХОТЯ БЫ ОДНА метка не NORMAL — это 1
            for label in labels:
                if label != '__label__NORMAL':
                    is_destructive = 1
                    break

            # 3. Записываем строку в CSV
            writer.writerow([is_destructive, text_part])
            count += 1

    print(f"Готово! Обработано строк: {count}. Результат в файле: {output_file}")


if __name__ == '__main__':
    process_file()