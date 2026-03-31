from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import os


bronze_dir = 'data/bronze/'
silver_dir = 'data/silver/'


def get_spark():
    
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName('Silver Layer')\
            .getOrCreate()
            
    return spark


def load_bronze_tables(spark, bronze_dir):
    
    tabelas = os.listdir(bronze_dir)
    print('Tabelas encontradas:', tabelas)
    dfs = {}   

    for tabela in tabelas:
        path_tabela = os.path.join(bronze_dir, tabela)
        if os.path.isdir(path_tabela):
            print('Processando:', tabela)
            df = spark.read.parquet(path_tabela)
            dfs[tabela]= df
            print(f'Lido: {tabela}')
            
    print(f'Todas as tabelas carregadas: {list(dfs.keys())}')
    return dfs
    
    
def transform_orders(df):

    cols_data = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]

    for c in cols_data:
        df = df.withColumn(c, to_timestamp(col(c)))

    return df.where(col('order_id').isNotNull())\
        .dropDuplicates(['order_id'])
    
    
def transform_generic(df, key_col):

    return df.where(col(key_col).isNotNull())\
        .dropDuplicates([key_col])
    
    
def main():
    
    spark = get_spark()
    dfs = load_bronze_tables(spark, bronze_dir)    

    tabelas_para_processar = {
        'orders': 'order_id',
        'order_items': 'order_id',
        'products': 'product_id',
        'customers': 'customer_id',
        'payments': 'order_id',
        'category_translation': 'product_category_name'
    }


    for tabela, pk in tabelas_para_processar.items():
        if tabela in dfs:
            print(f'Processando: {tabela}')

            if tabela == 'orders':
                df_silver = transform_orders(dfs[tabela])
            else:
                df_silver = transform_generic(dfs[tabela], pk)
            
            output_path = os.path.join(silver_dir, tabela)
            df_silver.write.mode('overwrite').parquet(output_path)
            print(f"[SILVER] Salvo: {tabela} | Linhas: {df_silver.count()}")

    spark.stop()
    print('Processamento concluído!')


if __name__ == '__main__':
    main()
    
            