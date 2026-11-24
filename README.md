# BioLIMS — лабораторна робота №2

Навчальний приклад трьох bounded contexts: Intake, Assay та Validation/Report.
Збережено API `/api/orders`, `/api/assays/run`, `/api/reports` і ланцюжок трасування.

## Запуск

Замінити всю папку `BioLIMS_Lab2`, включно з новою папкою `common`, та відкрити термінал у ній.
Потрібен Docker з Compose v2.

Якщо запущена попередня версія, спочатку виконати `docker compose down` у її папці.
Це звільнить порти; дані старої версії зберігалися лише в оперативній пам'яті.

```sh
docker compose up -d --build --scale order-sample-service=3
docker compose ps
```

Після запуску дочекатися успішних health checks у Consul; вони виконуються раз на 5 секунд.
Короткочасна відповідь 503 до появи здорових реплік означає, що discovery ще не готовий.

- API Gateway: http://localhost:8080/api/orders
- Consul UI: http://localhost:8500
- RabbitMQ UI: http://localhost:15672 (навчальні `guest` / `guest`)

## Перевірка через реальну інфраструктуру

На комп'ютері з Python 3.11+ виконати з цієї папки:

```sh
python smoke_test.py --replicas 3
```

Сценарій чекає готовності сервісів, створює замовлення лише на GLU, робить 6 запитів,
показує 3 різні репліки OrderService, чекає звіт через RabbitMQ та перевіряє 404/409/422.
Для демонстрації послідовного циклу залишити одну репліку AssayService і не запускати
інші запити аналізів одночасно. Кожен запуск створює замовлення з новим штрихкодом.

Ручний приклад у PowerShell:

```powershell
$barcode = "LAB2-" + [guid]::NewGuid().ToString("N").Substring(0,12)
$body = @{ patient_id="PAT-NEW"; barcode=$barcode; biomarkers=@("GLU") } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/orders -ContentType application/json -Body $body
$run = @{ barcode=$barcode; initial_message="Lab 2 check" } | ConvertTo-Json
1..6 | ForEach-Object {
    Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/assays/run -ContentType application/json -Body $run
} | Select-Object selected_order_service, handled_by_order_instance, event_id
Start-Sleep -Seconds 2
Invoke-RestMethod "http://localhost:8080/api/reports/$barcode" | ConvertTo-Json -Depth 10
```

Якщо звіту ще немає, повертається `REPORT_NOT_AVAILABLE`: повторити GET пізніше.
Після `docker compose restart order-sample-service validation-report-service` ті самі
замовлення та звіти доступні знову. Звичайний `docker compose down` зберігає томи;
параметр `-v` видалить їх разом із даними.

## Що виправлено

- **Дані між репліками.** OrderService використовує `orders.sqlite3` у спільному томі
  `order-data`. ReportService має окрему БД у `report-data`. Усі репліки відповідного
  сервісу бачать однакові дані. UUID та унікальний штрихкод захищають від колізій;
  одночасні запити не перезаписують існуюче замовлення.
- **Справжній Service Discovery.** AssayService запитує Consul з `passing=true`,
  обирає адресу за Round Robin і викликає саме її. Сортування адрес усуває залежність
  від порядку відповіді Consul. Лічильник окремий у кожної репліки AssayService.
  Якщо GET до обраної репліки не вдався, перевіряються інші знайдені репліки.
- **Health checks.** Сервіси реєструють HTTP-перевірку `/health`, перевіряють статус
  відповіді Consul і знімають реєстрацію при штатному завершенні. ReportService
  повідомляє 503, якщо його споживач RabbitMQ не підключений. `/health` AssayService
  перевіряє сам HTTP-сервіс; недоступні зовнішні залежності обробляються в `/assays/run`.
- **Призначені аналізи.** Замовлення на GLU формує лише GLU. Порожні переліки,
  дублікати, невідомі біомаркери та некоректні штрихкоди відхиляються з 422.
- **Зрозумілі помилки.** Невідоме замовлення — 404, повторний штрихкод — 409,
  неправильна відповідь OrderService — 502, недоступні Consul/репліки/RabbitMQ — 503.
  Фіктивні `ORD-AUTO` і `PAT-DEMO` більше не створюються.
- **Доставка подій.** Durable-черга, persistent-повідомлення, publisher confirms та
  ручний ACK після запису звіту. `event_id` запобігає повторній обробці того самого
  повідомлення. Старі події не підміняють новіший звіт для штрихкоду. Помилка БД
  повертає подію в чергу; некоректна подія відхиляється та записується в журнал.
- **Чесна демонстрація.** Вимірювання фіксовані та мають `simulation: true`.
  DeltaCheck — `NOT_IMPLEMENTED`, КЕП — `NOT_SIGNED`, QR-перевірка — `null`.
  Вихід за демонстраційний інтервал позначається `OUTSIDE_REFERENCE_RANGE`.

Nginx маршрутизує зовнішні запити через Docker DNS із кешем на 1 секунду.
Це окремий механізм від явного client-side Round Robin у AssayService.
Заголовок `Connection: close` більше не видається за реалізацію балансування.

## Межі навчального прикладу

SQLite у спільному Docker-томі придатна для цієї демонстрації на **одному Docker-хості**.
Це свідоме спрощення: PostgreSQL із проєктної архітектури ЛР1 тут не реалізована.
Для розміщення реплік на різних серверах потрібна мережева БД.
Кожен контекст володіє своїм сховищем; AssayService у цьому прикладі генерує подію без
окремої БД. Повноцінні DeltaCheck, КЕП, PDF та інтеграція з аналізатором залишаються
за межами реалізації ЛР2.

Повторна доставка однієї події є ідемпотентною, але повторний POST `/assays/run`
створює новий запуск і новий `event_id`. Якщо підтвердження брокера загубилося,
503 не доводить, що подію не доставлено. Тут немає transactional outbox або гарантії
exactly-once. Старі події попередньої версії без нових обов'язкових полів відхиляються.

## Компонентні тести

```sh
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\python.exe -m pip install -r requirements-test.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
# Linux/macOS:
# .venv/bin/python -m pip install -r requirements-test.txt
# .venv/bin/python -m unittest discover -s tests -v
```

17 тестів перевіряють FastAPI та реальну SQLite, зокрема конкурентне створення,
доступ між репліками, Round Robin, фільтрацію аналізів, обробку помилок,
ідемпотентність подій і ACK після COMMIT. Мережеві звернення до Consul та RabbitMQ
замінені тестовими об'єктами. Ці тести пройдені на Python 3.12.
Docker-образ використовує Python 3.11; повний Docker-стек у середовищі підготовки
не запускався. Для нього додано `smoke_test.py`.

## Основні файли

- `order-sample-service/app.py` — збереження замовлень та Open Host Service.
- `assay-execution-service/app.py` — синхронний виклик і публікація події.
- `validation-report-service/app.py` — споживач RabbitMQ та ACL звіту.
- `common/consul.py` — реєстрація, health checks, Round Robin.
- `common/contracts.py` — перевірка даних запитів, відповіді та події.
- `common/storage.py` — з'єднання з SQLite та транзакції.
- `docker-compose.yml` — сервіси, залежності, мережа й постійні томи.
