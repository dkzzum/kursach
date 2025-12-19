from pathlib import Path


def collect_py_files(directory, output_file):
    """Читает все .py файлы и записывает в txt"""

    dir_path = Path(directory)

    # Проверка что директория существует
    if not dir_path.exists():
        print(f"Директория {directory} не найдена")
        return

    # Получаем все .py файлы
    py_files = sorted(dir_path.glob("*.py"))

    if not py_files:
        print("Python файлы не найдены")
        return

    # Записываем в txt
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for py_file in py_files:
            outfile.write(f"\n{'=' * 60}\n")
            outfile.write(f"FILE: {py_file.name}\n")
            outfile.write(f"{'=' * 60}\n\n")

            try:
                with open(py_file, 'r', encoding='utf-8') as infile:
                    outfile.write(infile.read())
                outfile.write("\n\n")
            except Exception as e:
                outfile.write(f"[ОШИБКА ЧТЕНИЯ]: {e}\n\n")

    print(f"✓ Готово! Результат в {output_file} ({len(py_files)} файлов)")


# Использование
collect_py_files("../src/recommenders", "all_python_files.txt")
