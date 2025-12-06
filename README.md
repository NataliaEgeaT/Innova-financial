# Innova-financial

Este proyecto implementa un **Pipeline de Datos Financiero** para la empresa ficticia **Innova**, una SaaS que necesita visibilidad financiera precisa y confiable.  
El objetivo es construir un **Data Warehouse moderno** con **capas RAW → STG → PRD**, aplicando buenas prácticas de ingeniería de datos.

Incluye:

- Orquestación con **Apache Airflow**  
- Base de datos **DuckDB**  
- Infraestructura reproducible con **Docker Compose**

---

## 🔧 Cómo ejecutar el proyecto

### 1️⃣ Clonar el repositorio

git clone https://github.com/NataliaEgeaT/Innova-financial.git
cd Innova-financial



### 2️⃣ Ejecutar de manera local (sin Docker)

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt

python -m etl.pipeline

### 3️⃣ Levantar Airflow localmente


docker-compose up --build

http://localhost:8085

Credenciales:

user: airflow
pass: airflow