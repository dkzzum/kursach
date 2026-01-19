import os
import base64
from dash import Dash, html
import dash_bootstrap_components as dbc

# Путь к отчетам (внутри контейнера)
REPORT_DIR = "/data/reports"

# Инициализация приложения
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])


def get_image_cards():
    """Читает все PNG файлы из папки и создает из них карточки"""
    image_files = sorted([f for f in os.listdir(REPORT_DIR) if f.endswith('.png')])

    cards = []
    for filename in image_files:
        filepath = os.path.join(REPORT_DIR, filename)

        # Кодируем изображение в base64 для отображения
        with open(filepath, "rb") as f:
            encoded_image = base64.b64encode(f.read()).decode("ascii")

        card = dbc.Card(
            [
                dbc.CardImg(src=f"data:image/png;base64,{encoded_image}", top=True),
                dbc.CardBody(
                    [
                        html.H5(filename, className="card-title"),
                    ]
                ),
            ],
            className="mb-4",
        )
        cards.append(dbc.Col(card, width=12))  # Одна картинка во всю ширину

    if not cards:
        return [html.H3("⚠️ Отчеты еще не сгенерированы. Запустите пайплайн.")]

    return cards


app.layout = dbc.Container(
    [
        html.H1("📊 Аналитический Дашборд (Big Data Project)", className="text-center my-4"),
        html.Hr(),
        dbc.Row(get_image_cards()),
    ],
    fluid=True
)


if __name__ == '__main__':
    # Запускаем сервер на 0.0.0.0, чтобы он был доступен снаружи Docker
    print(f"Запуск веб-сервера. Откройте http://localhost:8050")
    app.run(debug=True, host='0.0.0.0', port=8050)
