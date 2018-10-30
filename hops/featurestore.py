"""
TODO
"""

from hops import util
from hops import hdfs


def project_featurestore():
    """
    Gets the project's featurestore name (project_featurestore)

    Returns:
        the project's featurestore name

    """
    project_name = hdfs.project_name()
    featurestore_name = project_name + "_featurestore"
    return featurestore_name


def use_featurestore(spark, featurestore=project_featurestore()):
    """
    Selects the featurestore database in Spark

    Args:
        spark: the spark session
        featurestore: the name of the database, defaults to the project's featurestore

    Returns:
        None

    """
    spark.sql("use " + featurestore)


def get_featuregroup(featuregroup, featurestore=project_featurestore(), featuregroup_version = 1):
    """
    Gets a featuregroup from a featurestore as a spark dataframe

    Args:
        featuregroup: the featuregroup to get
        featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        featuregroup_version: (Optional) the version of the featuregroup

    Returns:
        a spark dataframe with the contents of the featurestore

    """
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    return spark.sql("SELECT * FROM " + featuregroup + "_" + str(featuregroup_version))


def get_feature(feature, featurestore=project_featurestore(), featuregroup=None, featuregroup_version = 1):
    """
    Gets a particular feature (column) from a featurestore, if no featuregroup is specified it queries hopsworks metastore
    to see if the feature exists in any of the featuregroups in the featurestore. If the user knows which featuregroup
    contain the feature, it should be specified as it will improve performance of the query.

    Args:
        feature: the feature name to get
        featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        featuregroup: (Optional) the featuregroup where the feature resides
        featuregroup_version: (Optional) the version of the featuregroup

    Returns:
        A spark dataframe with the feature

    """
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    if (featuregroup != None):
        return spark.sql("SELECT " + feature + " FROM " + featuregroup + "_" + str(featuregroup_version))
    else:
        # make REST call to find out where the feature is located and return them
        # if the feature exists in multiple tables return an error message specifying this
        pass
    pass


def get_features(features, featurestore=project_featurestore(), featuregroup=None, featuregroup_version = 1):
    """
    Gets a list of features (columns) from the featurestore. If no featuregroup is specified it will query hopsworks
    metastore to find where the features are stored.

    Args:
        features: a list of features to get from the featurestore
        featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        featuregroup: (Optional) the featuregroup where all the features resides
        featuregroup_version: (Optional) the version of the featuregroup

    Returns:
        A spark dataframe with all the features

    """
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    if (featuregroup != None):
        featuresStr = features.join(", ")
        return spark.sql("SELECT " + featuresStr + " FROM" + featuregroup + "_" + str(featuregroup_version))
    else:
        # make REST call to find out where the feature is located and return them
        # if the feature exists in multiple tables return an error message specifying this
        pass
    pass


def query_featurestore(query, featurestore=project_featurestore()):
    """
    Executes a generic SQL query on the featurestore
    Args:
        query: SQL query
        featurestore: the featurestore to query, defaults to the project's featurestore

    Returns:
        A dataframe with the query results

    """
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    return spark.sql(query)


def insert_into_featuregroup(sparkDF, featuregroup, featurestore=project_featurestore(), mode="append", featuregroup_version=1):
    """
    Saves the given dataframe to the specified featuregroup. Defaults to the project-featurestore

    Args:
        sparkDF: the dataframe containing the data to insert into the featuregroup
        featuregroup: the name of the featuregroup (hive table name)
        featurestore: the featurestore to save the featuregroup to (hive database)
        mode: the write mode (defaults to "append") available modes are: "append", "overwrite", "ignore", "error", and "errorifexists", see: https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes
        featuregroup_version: the version of the featuregroup (defaults to 1)

    Returns:
        None

    """
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    sparkDF.write.format("ORC").mode(mode).saveAsTable(featuregroup + "_" + str(featuregroup_version))
