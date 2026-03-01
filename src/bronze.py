from pyspark.sql import SparkSession


def get_spark():
    
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName('Olist Bronze Layer')\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
    
    return spark


def main():
    spark = get_spark()
    
    arquivos = {
        "olist_orders_dataset.csv": "orders",
        "olist_order_items_dataset.csv": "order_items",
        "olist_products_dataset.csv": "products",
        "olist_customers_dataset.csv": "customers",
        "olist_order_payments_dataset.csv": "payments",
        "product_category_name_translation.csv": "category_translation",
}
    
    for nome_arquivo, nome_tabela in arquivos.items():
        
        path_entrada = f'data/raw/olist/{nome_arquivo}'
        path_saida = f'data/bronze/{nome_tabela}'
        
        df = (
            spark.read
            .option('header', True)
            .option('inferSchema', True)
            .csv(path_entrada)
        )
        
        df.write.mode('overwrite').parquet(path_saida)
        
        print(f'Tabela {nome_tabela} criada com {df.count()} linhas')
        
        
        spark.stop()
        
    
if __name__ == '__main__':
    main()