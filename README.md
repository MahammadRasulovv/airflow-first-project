# Airflow: CSV → Postgres pipeline

Docker Compose ilə qaldırılan Airflow 3 layihəsi. DAG `products.csv` faylını yoxlayır, varsa Postgres-ə yazır və bitəndə mail göndərir.

## Axın

```
check_products_file  →  load_csv_to_postgres  →  send_result_mail
   (FileSensor)           (PythonOperator)        mail_success / mail_failed
```

| Tapşırıq | Nə edir |
|---|---|
| `check_products_file` | `/opt/airflow/files/products.csv` faylını yoxlayır (30 san-dən bir, 5 dəq timeout) |
| `load_csv_to_postgres` | CSV-ni `products` cədvəlinə yazır (hər icrada cədvəl yenidən qurulur) |
| `mail_success` | Əvvəlki tapşırıq uğurlu olsa mail göndərir (`all_success`) |
| `mail_failed` | Əvvəlki tapşırıq fail olsa mail göndərir (`one_failed`) |

**Parametrlər:** schedule `0 */4 * * *` (4 saatdan bir, UTC), error olsa 3 dəfə təkrar, təkrarlar arası 5 dəqiqə. Mail tapşırıqları təkrar olunmur (`retries=0`).

## Fayl strukturu

```
airflow/
├── airflow/
│   ├── dags/
│   │   ├── writecsv.py          # ilk versiya (COPY məqsədli error verir)
│   │   └── writecsvsucces.py    # işləyən versiya (CSV həqiqətən yazılır)
│   ├── files/products.csv       # mənbə faylı
│   ├── logs/  config/  plugins/
├── .env                         # parollar və Airflow ayarları
├── docker-compose.yaml          # postgres, minio, airflow servisləri
├── Dockerfile                   # apache/airflow:3.1.0 + requirements
└── requirements.txt             # provider paketləri
```

Hər DAG-ın `dag_id`-si **unikal** olmalıdır, yoxsa Airflow biri görünmür.

## İşə salmaq

```powershell
docker compose up -d        # servisləri qaldır
docker compose build        # Dockerfile / requirements dəyişəndə
```

UI: http://localhost:8080 (Postgres host portu `5436`, MinIO `9000`/`9001`; MinIO bu DAG-larda istifadə olunmur).

## Connections (Admin → Connections)

| Connection ID | Növ | Əsas ayarlar |
|---|---|---|
| `fs_default` | File (path) | Extra: `{"path": "/"}` (və ya `.env`-də `AIRFLOW_CONN_FS_DEFAULT='fs://?path=/'`) |
| `pg_code` | Postgres | Host `postgres`, Port `5432`; Login, Password, Database `.env`-dəki `POSTGRES_*` dəyərləridir |
| `smtp_default` | SMTP | Host `smtp.gmail.com`, Port `587`, Login Gmail ünvanı, Password **Gmail App Password**; Extra: `from_email`, `disable_ssl: true`, `disable_tls: false` |

Connection ID-lər koddakı adlarla eyni olmalıdır. App Password və hesab parolunu fayllarda saxlama.

## Niyə `copy_expert`?

`COPY ... FROM '/yol/fayl.csv'` faylı **Postgres konteynerində** axtarır, fayl isə **Airflow konteynerindədir**. Ona görə Python faylı Airflow tərəfdə oxuyur və `PostgresHook.copy_expert` ilə Postgres-ə göndərir.

## Yoxlama

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM products;"
docker compose exec airflow-scheduler airflow dags list-import-errors
```

## Tez-tez rast gəlinən problemlər

| Əlamət | Səbəb / həll |
|---|---|
| Sensor dərhal fail olur | `fs_default` connection-ı yoxdur |
| `password authentication failed` | `pg_code`-da login/parol/database `.env` ilə uyğun deyil |
| `SMTP connection is not found` | `smtp_default` yaradılmayıb |
| SMTP `timed out` / `unexpectedly closed` | Port `587` olmalıdır, `disable_ssl: true`, parol App Password olmalıdır |
| Yeni DAG siyahıda görünmür | `dag_id` təkrarlanır, ya da faylda import error var; 1-5 dəqiqə gözlə |
| Task uzun müddət `Up For Retry` | Normaldir, `retry_delay` (5 dəq) gözlənilir |
