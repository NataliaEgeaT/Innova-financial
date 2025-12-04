import os
import pandas as pd
from datetime import datetime

RAW_DIR = "data/raw"

REQUIRED_FILES = {
    "customers": "customers.csv",
    "transactions": "transactions.csv",
    "payments": "payments.csv",
    "expenses": "expenses.csv",
    "employees": "employees.csv",
    "subscriptions": "subscriptions.csv"
}


def _check_file_exists(filepath: str):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"❌ Archivo no encontrado: {filepath}")
    print(f"✔ Archivo encontrado: {filepath}")


def _load_csv(filepath: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(filepath)
        print(f"✔ CSV cargado correctamente: {filepath} ({len(df)} filas)")
        return df
    except Exception as e:
        raise Exception(f"❌ Error leyendo CSV {filepath}: {str(e)}")


def _validate_duplicates(df: pd.DataFrame, pk: str, table_name: str):
    if pk not in df.columns:
        print(f"⚠ Advertencia: la columna PK '{pk}' no existe en {table_name}")
        return
    
    duplicates = df[pk].duplicated().sum()
    if duplicates > 0:
        raise Exception(f"❌ {table_name} contiene {duplicates} duplicados en '{pk}'")
    
    print(f"✔ No hay duplicados en {table_name}.{pk}")


def extract_csvs():
    dfs = {}
    print("\n INICIANDO EXTRACCIÓN DE CSV DESDE data/raw/\n")

    for table, filename in REQUIRED_FILES.items():
        filepath = os.path.join(RAW_DIR, filename)

        # 1. Validar existencia
        _check_file_exists(filepath)

        # 2. Cargar
        df = _load_csv(filepath)

        # 3. Validación de duplicados según tabla
        if table == "customers":
            _validate_duplicates(df, "customer_id", table)

        if table == "transactions":
            _validate_duplicates(df, "transaction_id", table)

        if table == "payments":
            _validate_duplicates(df, "payment_id", table)

        if table == "expenses":
            _validate_duplicates(df, "expense_id", table)

        if table == "employees":
            _validate_duplicates(df, "employee_id", table)

        if table == "subscriptions":
            _validate_duplicates(df, "subscription_id", table)

        # 4. Limpieza básica inicial
        df.columns = df.columns.str.lower().str.strip()

        dfs[table] = df
    
    print("\n EXTRACCIÓN COMPLETADA EXITOSAMENTE.\n")
    return dfs
