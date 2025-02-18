import csv
import time
import os
import uuid
from datetime import datetime
from faker import Faker

# Инициализация Faker
fake = Faker()

# Путь к папке проекта
project_folder = os.path.join(os.getcwd(), "generated_csv")

# Создание отдельных папок для ads, leads и purchases
ads_folder = os.path.join(project_folder, "ads")
leads_folder = os.path.join(project_folder, "leads")
purchases_folder = os.path.join(project_folder, "purchases")

# Создание папок, если они не существуют
for folder in [project_folder, ads_folder, leads_folder, purchases_folder]:
    if not os.path.exists(folder):
        os.makedirs(folder)


# Функция для генерации одной строки данных для ads.csv
def generate_ads_row():
    return [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        f"xo-for-client-{fake.word()}",
        fake.random_element(["yandex", "google", "facebook", "vk", "instagram"]),
        fake.random_element(["cpc", "cpm", "organic", "email"]),
        fake.random_int(min=10000000, max=99999999),
        fake.random_int(min=1000000000, max=9999999999),
        "",
        round(fake.random.uniform(0.1, 10.0), 2),
        round(fake.random.uniform(10.0, 100.0), 2),
    ]


# Функция для генерации одной строки данных для leads.csv
def generate_leads_row():
    return [
        datetime.now().strftime("%Y-%m-%d"),
        str(uuid.uuid4()),
        fake.random_element(["yandex", "google", "facebook", "vk", "instagram", "sms", "email", ""]),
        fake.random_element(["cpc", "cpm", "organic", "email", "", None]),
        fake.random_int(min=1, max=9999) if fake.boolean() else "",
        "" if fake.boolean() else fake.random_int(min=1000000000, max=9999999999),
        "",
        str(uuid.uuid4()),
    ]


# Функция для генерации одной строки данных для purchases.csv
def generate_purchases_row():
    return [
        datetime.now().strftime("%Y-%m-%d"),
        str(uuid.uuid4()),
        str(uuid.uuid4()),
        round(fake.random.uniform(500.0, 10000.0), 2),
    ]


# Функция для генерации CSV-файла
def generate_csv(folder, filename, headers, row_generator, rows=100):
    file_path = os.path.join(folder, filename)
    data = [row_generator() for _ in range(rows)]
    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)
    print(f"Файл {file_path} успешно создан.")


# Основной цикл
def main():
    while True:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Генерация ads.csv
        generate_csv(
            ads_folder,
            f"{timestamp}_ads.csv",
            ["created_at", "d_ad_account_id", "d_utm_source", "d_utm_medium", "d_utm_campaign", "d_utm_content",
             "d_utm_term", "m_clicks", "m_cost"],
            generate_ads_row
        )

        # Генерация leads.csv
        generate_csv(
            leads_folder,
            f"{timestamp}_leads.csv",
            ["lead_created_at", "lead_id", "d_lead_utm_source", "d_lead_utm_medium", "d_lead_utm_campaign",
             "d_lead_utm_content", "d_lead_utm_term", "client_id"],
            generate_leads_row
        )

        # Генерация purchases.csv
        generate_csv(
            purchases_folder,
            f"{timestamp}_purchases.csv",
            ["purchase_created_at", "purchase_id", "client_id", "m_purchase_amount"],
            generate_purchases_row
        )

        # Пауза между генерациями
        time.sleep(10)


if __name__ == "__main__":
    main()