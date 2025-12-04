from etl.extract import extract_csvs
from etl.load import load_to_staging, build_dimensions_layer, build_facts_layer


def run_pipeline():

    # 1. Validación y lectura de CSV
    extract_csvs()

    # 2. Carga a STAGING en DuckDB
    load_to_staging()

    # 3. Construcción de dimensiones
    build_dimensions_layer()

    # 4. Construcción de hechos
    build_facts_layer()

    print("\n PIPELINE COMPLETADO CON EXITO\n")


if __name__ == "__main__":
    run_pipeline()


