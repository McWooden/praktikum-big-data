"""Paket praktikum_big_data dengan helper Spark otomatis."""

def main() -> None:
    print("Hello from praktikum-bigdata!")

def get_spark():
    from spark_init import spark, F, Window
    return spark, F, Window
