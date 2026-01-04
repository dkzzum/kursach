import os
import matplotlib.pyplot as plt
import seaborn as sns

# Настройки
plt.rcParams.update({'figure.figsize': (10, 6), 'figure.dpi': 100})
sns.set_theme(style="whitegrid")
REPORT_DIR = "/data/reports"


def draw_scalability_chart():
    # ДАННЫЕ (Объем данных vs Время в секундах)
    # 30к строк -> 5 сек
    # 300к строк -> 5.5 сек
    # 3.2млн строк -> 42 сек
    data_sizes = [32854, 326605, 3270670]
    times = [5.29, 5.43, 41.70]

    labels = ["Small\n(30k)", "Medium\n(320k)", "Big Data\n(3.2M)"]

    plt.figure()
    plt.plot(labels, times, marker='o', linestyle='-', color='b', linewidth=2, markersize=8)

    plt.title('Масштабируемость Spark ML (Линейная зависимость)', fontsize=14)
    plt.xlabel('Объем данных')
    plt.ylabel('Время обучения (секунды)')
    plt.grid(True)

    # Подписи
    for i, txt in enumerate(times):
        plt.annotate(f"{txt} сек", (labels[i], times[i]), textcoords="offset points", xytext=(0, 10), ha='center')

    save_path = os.path.join(REPORT_DIR, "6_scalability_time.png")
    plt.savefig(save_path)
    print(f"✅ График готов: {save_path}")


if __name__ == "__main__":
    draw_scalability_chart()
