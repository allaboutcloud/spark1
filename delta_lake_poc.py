

import os
os.system("sudo pip install boto3")
os.system("sudo cp delta-core_2.11-0.6.1.jar /usr/lib/spark/jars/")
from pyspark.sql import *
from pyspark.sql.types import *
#from delta import *
#from delta.tables import *
from pyspark.sql.functions import *
import boto3 

if __name__ == "__main__":




#    parser = argparse.ArgumentParser()
#    parser.add_argument('entity', help='entity name', type=int)
    #parser.add_argument('year', help='execution year', type=int)
    #parser.add_argument('month', help='execution month', type=int)
    #parser.add_argument('day', help='execution day', type=int)
    #parser.add_argument('opco', help='opco to be executed', type=str)
    #args = parser.parse_args()
    #assert args.opco in ('ES', 'UK', 'CSA'), 'Invalid OpCo code. Please input CSA, ES or UK to proceed.'
    ### read lookup file for enrity
    print("working on checking prefix in s3")


    client = boto3.client('s3')

    spark = SparkSession \
        .builder \
        .appName("DeltaLake") \
        .config("spark.jars", "/usr/lib/spark/jars/delta-core_2.11-0.6.1")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


    spark.sparkContext.addPyFile("/usr/lib/spark/jars/delta-core_2.11-0.6.1.jar")
    spark.sparkContext.setLogLevel("ERROR")

    from delta import *
    from delta.tables import *

    entity_name="1gnite_account"
    datalake_bucket="s3://vf-bdc-vb-euce1-preprod-data/"
   # account="s3://vf-bdc-vb-euce1-preprod-data/datalake/"+entity_name+"/year=2018/"
    #entity_source_location="s3://vf-bdc-vb-euce1-preprod-data/datalake/"+entity_name+"/year=2021/month=9/"
    #entity_target_location="s3://get-leap-preprod-emr-temp/deltalake/"+entity_name
   # df=spark.read.format("parquet").load(entity_source_location)
    #number_of_records=df.count()

    load="base_load"
    parquet_file_name=[]
########################## WRITING for firsttime ###################
    if ( load=="delta_load" ):
        response = client.list_objects_v2(Bucket='vf-bdc-vb-euce1-preprod-data',Prefix='datalake/'+entity_name+"/year=2021/month=9/")
#print(response)
        for k,v in response.items():
            if (k=="KeyCount"):
                print('++++++++++++++++++++'+k+'++++++++++'+str(v)+'++++++++++++++++++++++++++++++++')
            if (k=="Contents"):
                for key in v:
                    for keys,values in key.items():
                        if (keys=="Key"):
                            if(values.endswith("parquet")):
                                #print(values)
                                parquet_file_name.append(values)

#+++++++++++++
#+++++++++++++
#    for filename in parquet_file_name:
#        print("filenames are: ",filename)
#+++++++++++++
#+++++++++++++
#    load="base_load"
    if ( load=="full_load" ):
        entity_source_location="s3://vf-bdc-vb-euce1-preprod-data/datalake/"+entity_name+"year=2021/month=2/"
        base_file_df=spark.read.format("parquet").option("header","true").load(entity_source_location)
        base_df.write.format("delta").mode("overwrite").save(entity_target_location)
        delta_df=spark.read.format("delta").load(entity_target_location)
       # print("The dataframe type {}".format(delta_df.dtypes))
       # print("The delta schema is {}".format(delta_df.schema))
#    df.write.format("delta").mode("overwrite").saveAsTable("OPPOTUNITY")
#    spark.sql("SELECT * FROM OPPOTUNITY")
        entity_delta= DeltaTable.forPath(spark, entity_target_location)
        delta_df_count=spark.read.format("delta").load(entity_target_location)
        print("count of records in delta {}".format(str(delta_df_count.count())))

        #fullHistoryDF = entity_delta.history(1)
        #fullHistoryDF.show()
    if ( load=="delta_load" ):
        for filename in parquet_file_name:
            print("filenames are: ",filename)
            source_file_df=spark.read.format("parquet").option("header","true").load(datalake_bucket+filename)
            #product_df1.printSchema()
            entity_delta= DeltaTable.forPath(spark, entity_target_location)
            entity_delta.alias("baseload").merge(
            source_file_df.alias("newdata"),
            "baseload.Id=newdata.Id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            delta_df_count=spark.read.format("delta").load(entity_target_location)
            print("count of records in delta {}".format(str(delta_df_count.count())))