import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark():
    return SparkSession.builder \
            .master('local[*]') \
            .appName('Olist Gold Layer') \
            .getOrCreate()


def main():
    spark = get_spark()
    silver_dir = 'data/silver'
    gold_dir = 'data/gold'

    print('--- Lendo Tabelas Silver ---')

    df_orders = spark.read.parquet(f'{silver_dir}/orders')
    df_items = spark.read.parquet(f'{silver_dir}/order_items')
    df_products = spark.read.parquet(f'{silver_dir}/products')
    df_customers = spark.read.parquet(f'{silver_dir}/customers')

    print('--- Construindo a Tabela Fato: f_vendas ---')
    

    df_gold = df_items.join(df_orders, 'order_id', 'inner') \
                      .join(df_products, 'product_id', 'left') \
                      .join(df_customers, 'customer_id', 'left')


    df_gold = df_gold.select(
        'order_id',
        'order_purchase_timestamp',
        'product_category_name',
        'customer_city',
        'customer_state',
        'price',
        'freight_value',

        F.datediff('order_delivered_customer_date', 'order_purchase_timestamp')
        .alias('dias_entrega_real'),

        (F.col('price') + F.col('freight_value')).alias('valor_total_item')
    )


    output_path = os.path.join(gold_dir, 'f_vendas_detalhadas')
    df_gold.write.mode('overwrite').parquet(output_path)

    print(f'--- Camada Gold Concluída! Total de registros: {df_gold.count()} ---')
    
    spark.stop()


if __name__ == '__main__':
    main()       
        
        
