from pyspark import SQLContext
from pyspark.sql import SparkSession
from datetime import date, datetime
import time
import os
import pyspark.sql.functions as F
import pyspark.pandas as ps
import pymysql

mysql_host = 'localhost'
mysql_db = 'ershoufang'
mysql_user = 'root'
mysql_pwd = '666666'
mysql_table = 'table_data'


def database():
    ershoufang = pymysql.Connect(
        host=mysql_host,
        user='root',
        password='666666',
        database='ershoufang'
    )
    dic = {
        'all_results': 'index',
        'new_results': 'index',
        'visualize_data': 'level_0',
    }
    cursor = ershoufang.cursor()
    # 执行sql语句
    for table_name, attribute_name in dic.items():
        sql = "alter table `{}` add primary key (`{}`);".format(table_name, attribute_name)
        cursor.execute(sql)

    ershoufang.close()
    cursor.close()


def Get_Table_data(origin_path, target_path):
    data = ps.read_csv(origin_path, encoding='utf-8')
    Table_data = ps.DataFrame(
        index=["总数", "行政区个数", "最多户型", "最多装修", "最高价格", "最低价格", "平均总价", "平均平米单价", "平均面积", "房价最大影响因素"], columns=["value"],
        dtype='object')
    Table_data.loc["总数", "value"] = int(data["总数"].max())
    Location_houses_summary = data.loc[:, '所在区'].value_counts()
    Table_data.loc["行政区个数", "value"] = int(len(Location_houses_summary.index))
    Table_data.loc["最多户型", "value"] = data.loc[:, "户型"].value_counts().index.to_list()[0]
    Table_data.loc["最多装修", "value"] = data.loc[:, "装修情况"].value_counts().index.to_list()[0]
    Table_data.loc["最高价格", "value"] = int(data.loc[:, "总价"].max())
    Table_data.loc["最低价格", "value"] = int(data.loc[:, "总价"].min())
    Table_data.loc["平均总价", "value"] = int(data.loc[:, "总价"].mean())
    Table_data.loc["平均平米单价", "value"] = int(data.loc[:, "平米单价"].mean())
    Table_data.loc["房价最大影响因素", "value"] = "square"
    res = data.loc[:, "面积"].to_list()
    squares = []
    for item in res:
        squares.append(float(item[:-1]))
    Table_data.loc["平均面积", "value"] = int(sum(squares) // len(squares))
    # print(Table_data)
    ps_df = Table_data.reset_index()
    pd_df = ps_df.to_pandas()
    pd_df.to_csv(target_path, encoding='utf-8', index=False)
    spark_df = ps_df.to_spark()
    # spark_df.show()
    return spark_df


def Devided_data(origin_path, target_path):
    data = ps.read_csv(origin_path, encoding='utf-8')
    Devided_data = ps.DataFrame(columns=['档次', '总数'], dtype='object')
    a = data["总价"]
    res1 = len(a[a > 130])
    res2 = len(a[a > 180])
    dicts = [["gao", res2], ["zhong", res1 - res2], ["di", len(a) - res1]]
    for i in range(3):
        Devided_data.loc[i] = dicts[i]
    Devided_data.to_csv(target_path, index=False)
    spark_df = Devided_data.to_spark()
    return spark_df


def sparkConfig():
    """
    spark的配置
    :return:
    """
    spark = SparkSession \
        .builder \
        .appName('data_to_mysql') \
        .master('local[*]') \
        .getOrCreate()

    sql = SQLContext(spark)
    return sql, spark


def DropDuplicates(path):
    df = ps.read_csv(path, encoding='utf-8')
    spark_df = df.to_spark()
    spark_df = spark_df.dropDuplicates(['总价', '平米单价', '小区', '所在区', '户型', '楼层',
       '面积', '户型结构', '建筑类型', '朝向', '建筑结构', '装修情况', '梯户比例', '电梯与否', '发布年',
       '发布月', '发布日'])
    ps_df = spark_df.to_pandas_on_spark()
    ps_df = ps_df.to_pandas()
    ps_df.to_csv(path, index=False)
    return spark_df


def Get_Visualize_data(origin_path, target_path):
    data = ps.read_csv(origin_path)
    Location_houses_summary = data.loc[:, '所在区'].value_counts().to_frame()
    avg_prices = ps.DataFrame(index=Location_houses_summary.index.to_list(), columns=["平均房源价格"])  # 初始为空
    for L_index in Location_houses_summary.index.to_list():
        all_indices = data[data['所在区'] == L_index].index.to_list()  # 找某区的所有index
        prices = data.loc[all_indices, "总价"]  # 得到所有的价格
        avg_price = int(prices.mean())
        avg_prices.loc[L_index, "平均房源价格"] = avg_price
    print(avg_prices)
    Location_houses_summary.columns = ["房源总数"]
    ps_df1 = Location_houses_summary.reset_index()
    ps_df2 = avg_prices.reset_index()
    Visualization_data = ps.merge(ps_df1, ps_df2, how="outer")  # 合并
    Visualization_data["index"] = ["双流区","大邑县","天府新区","崇州市","彭州市","成华区","新津区","新都区","武侯区","温江区","简阳市","郫都区","都江堰市","金堂县","金牛区",
                                   "锦江区","青白江区","青羊区","高新区","高新西区","龙泉驿区"]
    # Visualization_data[0] = Visualization_data[0].astype('int64')
    pd_df = Visualization_data.to_pandas()
    pd_df.to_csv(target_path, encoding='utf-8', index=pd_df.index.to_list())  # save
    ps_df = Visualization_data.reset_index()
    spark_df = ps_df.to_spark()
    # spark_df.show() #sparktype
    return spark_df
    Visualization_data.to_csv(target_path, encoding='utf-8', index=Location_houses_summary.index.to_list(),
                              header=["房源总数", "平均房源价格"])  # 包括索引的保存
    


def writetoMysql(url, properties, spark_df, table, mode):
    spark_df.write.jdbc(url=url, table=table, mode=mode, properties=properties)


def readCsv(spark):
    all_results = DropDuplicates("./all_results.csv")
    Table_data = Get_Table_data("./all_results.csv", "./Table_data.csv")
    Visualize_data = Get_Visualize_data("./all_results.csv", "./Visualize_data.csv")
    new_results = DropDuplicates("./new_results.csv")
    all_results.printSchema()
    Table_data.printSchema()
    Visualize_data.printSchema()
    new_results.printSchema()

    # Visualize_data.select(F.col('old_field1').alias('new_field1'), F.col('new_field2').alias('new_field2')).show()
    # data = data.withColumnRenamed('old_field', 'new_field')

    url = 'jdbc:mysql://{}:3306/{}?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&useSSL=false' \
          '&serverTimezone=GMT'.format(
        mysql_host, mysql_db)  # 有中文，需处理中文乱码
    auth_mysql = {"user": mysql_user, "password": mysql_pwd}
    writetoMysql(url=url, spark_df=all_results, table="all_results", mode="overwrite", properties=auth_mysql)
    writetoMysql(url=url, spark_df=new_results, table="new_results", mode="overwrite", properties=auth_mysql)
    writetoMysql(url=url, spark_df=Table_data, table="table_data", mode="overwrite", properties=auth_mysql)
    writetoMysql(url=url, spark_df=Visualize_data, table="visualize_data", mode="overwrite", properties=auth_mysql)
    database()

    # Table_data.write.jdbc(url, table="table_data", mode='overwrite', properties=auth_mysql)
    # Visualize_data.write.jdbc(url, table="visualize_data", mode="overwrite", properties=auth_mysql)
    # new_results.write.jdbc(url, table="visualize_data", mode="overwrite", properties=auth_mysql)
    """
        * `append`: 数据追加
        * `overwrite`: 如果数据存在则覆盖
        * `error`: 如果数据已经存在，则抛出异常。
        * `ignore`: 如果数据已经存在，则忽略此操作。
    """


# 会自动对齐字段，也就是说，df 的列不一定要全部包含MySQL的表的全部列才行


def main():
    sql, spark = sparkConfig()
    readCsv(spark)


if __name__ == '__main__':
    main()
