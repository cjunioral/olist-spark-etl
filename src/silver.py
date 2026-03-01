from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


bronze_dir = 'data/bronze/'
silver_dir = 'data/silver/'


def get_spark():
    
    spark = SparkSession.builder\
            .master('local[*]')\
            .appName('Silver Layer')\
            .config('spark.ui.port', '4050')\
            .getOrCreate()
            
    return spark


def load_bronze_tables(spark, bronze_dir):
    
    tabelas = os.listdir(bronze_dir)
    print('Tabelas encontradas:', tabelas)
    
    dfs = {}   

    for tabela in tabelas:
        path_tabela = os.path.join(bronze_dir, tabela)
        
        if os.path.isdir(path_tabela):
            print("Processando:", tabela)
            
            df = spark.read.parquet(path_tabela)
            
            dfs[tabela]= df
            
            print(f'Lido: {tabela}')
            
    print(f'Todas as tabelas carregadas: {list(dfs.keys())}')
            
    return dfs
    
    
def transform_orders(orders_df):
    
    orders_df = (
        orders_df.where(col('order_id').isNotNull()\
        ).where(col('customer_id').isNotNull()\
        ).dropDuplicates(['order_id'])
    )
    return orders_df
    
    
def transform_order_items(df_orders_items):
    
    df_orders_items = (
        df_orders_items.where(col('order_id').isNotNull()\
        ).where(col('product_id').isNotNull()\
        ).dropDuplicates(['order_id', 'order_item_id'])
    )
    
    return df_orders_items
    
    
def main():
    
    spark = get_spark()
    dfs = load_bronze_tables(spark, bronze_dir)
    
    
    orders_df = dfs['orders']
    
    linhas_antes = orders_df.count()
    df_orders_silver = transform_orders(orders_df)
    linhas_depois = df_orders_silver.count()
    
    print(f'{linhas_antes} Linhas antes e {linhas_depois} depois (order)')
    
    orders_out = os.path.join(silver_dir, 'orders')
    df_orders_silver.write.mode('overwrite').parquet(f'{silver_dir}orders')
    print(f'[orders] Salvo em: {orders_out}')
    
    
    order_items_df = dfs['order_items']
    
    linhas_antes_items = order_items_df.count()
    order_items_silver = transform_order_items(order_items_df)
    linhas_depois_items = order_items_silver.count()
    
    print(f'{linhas_antes_items} Linhas antes e {linhas_depois_items} depois (order_items)')
    
    order_items_out = os.path.join(silver_dir, 'order_items')
    order_items_silver.write.mode('overwrite').parquet(f'{silver_dir}order_items')
    print(f'[order_items] Salvo em: {order_items_out}')
    
    
    spark.stop()
    
    
if __name__ == '__main__':
    main()
    
            