import duckdb

def validate_row_counts():
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")  # por compatibilidad
    print("✔ Validación de conteo de filas completada (placeholder).")

def validate_nulls():
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;") 
    print("✔ Validación de valores nulos completada (placeholder).")
