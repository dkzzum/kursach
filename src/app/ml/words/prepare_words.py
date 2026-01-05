import json
import re


def convert_json_to_list(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # 1. Удаляем комментарии в стиле C/JS (/** ... */ и // ...)
        # Находим начало массива JSON '['
        start_index = content.find('[')
        if start_index != -1:
            json_content = content[start_index:]
        else:
            print("❌ Не удалось найти начало JSON массива '['")
            return

        # 2. Парсим JSON
        data = json.loads(json_content)

        # 3. Извлекаем слова и создаем список
        # Python автоматически декодирует \uXXXX в нормальные буквы
        toxic_list = [item['word'] for item in data]

        # 4. Формируем строку кода для копирования
        output_code = f"toxic_markers = {json.dumps(toxic_list, ensure_ascii=False, indent=4)}"

        # 5. Сохраняем в файл, чтобы удобно копировать
        with open("toxic_list_ready.py", "w", encoding="utf-8") as out:
            out.write(output_code)

        print(f"✅ Готово! Список из {len(toxic_list)} слов сохранен в файл 'toxic_list_ready.py'.")
        print("📋 Скопируйте содержимое этого файла и вставьте в 'app/ml/toxic_classifier.py'.")

    except Exception as e:
        print(f"⚠️ Ошибка: {e}")


if __name__ == "__main__":
    convert_json_to_list("words.json")