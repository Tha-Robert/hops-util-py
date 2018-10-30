"""
TODO
"""

from hops import util
from hops import hdfs

def project_featurestore():
    project_name = hdfs.project_name()
    featurestore_name = project_name + "_featurestore"
    return featurestore_name

def use_featurestore(spark, featurestore = project_featurestore()):
    spark.sql("use " + featurestore)

def get_featuregroup(featuregroup, featurestore = project_featurestore()):
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    return spark.sql("SELECT * FROM " + featuregroup)

def get_feature(feature, featurestore = project_featurestore(), featuregroup = None):
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    if(featuregroup != None):
        return spark.sql("SELECT " + feature + " FROM" + featuregroup)
    else:
        # make REST call to find out where the feature is located and return them
        # if the feature exists in multiple tables return an error message specifying this
        pass
    pass

def get_features(features, featurestore = project_featurestore(), featuregroup = None):
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    if(featuregroup != None):
        featuresStr = features.join(", ")
        return spark.sql("SELECT " + featuresStr + " FROM" + featuregroup)
    else:
        # make REST call to find out where the feature is located and return them
        # if the feature exists in multiple tables return an error message specifying this
        pass
    pass

def query_featuregroup(query, featuregroup, featurestore = project_featurestore()):
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    return spark.sql(query)

def save_featuregroup(sparkDF, overwrite=False):
    pass
