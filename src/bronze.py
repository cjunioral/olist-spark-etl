from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def get_spark():
    
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName('Olist Bronze Layer')\
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
        
        try:
            df = (
                spark.read
                .option('header', True)
                .option('inferSchema', True)
                .option('encoding', 'UTF-8')
                .csv(path_entrada)
            )

            df = df.withColumn("_ingestion_date", current_timestamp())
            
            df.write.mode('overwrite').parquet(path_saida)
            print(f'[BRONZE] Tabela {nome_tabela} processada com sucesso. {df.count()} linhas')

        except Exception as e:
            print(f'[BRONZE] Erro ao processar arquivo {nome_arquivo}: {e}')
        
            
    spark.stop()


if __name__ == '__main__':
    main()





