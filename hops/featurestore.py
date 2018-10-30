"""
TODO
"""

from hops import util
from hops import hdfs
from hops import tls
from hops import constants
import json

def project_featurestore():
    """
    Gets the project's featurestore name (project_featurestore)

    Returns:
        the project's featurestore name

    """
    project_name = hdfs.project_name()
    featurestore_name = project_name + "_featurestore"
    return featurestore_name

def _get_features_from_metastore(featurestore_name):
    json_contents = tls._prepare_rest_appservice_json_request()
    json_contents[constants.REST_CONFIG.JSON_FEATURESTORENAME] = featurestore_name
    json_embeddable = json.dumps(json_contents)
    headers = {'Content-type': 'application/json'}
    method = "POST"
    connection = util._get_http_connection(https=True)
    resource = constants.REST_CONFIG.HOPSWORKS_FEATURESTORE_RESOURCE
    resource_url = "/" + constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + "/" + constants.REST_CONFIG.HOPSWORKS_REST_APPSERVICE + "/" + resource
    connection.request(method, resource_url, json_embeddable, headers)
    response = connection.getresponse()
    resp_body = response.read()
    response_object = json.loads(resp_body)
    return response_object

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


def get_features(features, featurestore=project_featurestore(), featuregroups_version_dict={}, join_key=None):
    """
    Gets a list of features (columns) from the featurestore. If no featuregroup is specified it will query hopsworks
    metastore to find where the features are stored.

    Args:
        features: a list of features to get from the featurestore
        featurestore: the featurestore where the featuregroup resides, defaults to the project's featurestore
        featuregroups: (Optional) a dict with (fg --> version) for all the featuregroups where the features resides
        featuregroup_version: (Optional) the version of the featuregroup
        join_key: (Optional) column name to join on

    Returns:
        A spark dataframe with all the features

    """
    spark = util._find_spark()
    use_featurestore(spark, featurestore)
    if (len(featuregroups_version_dict) > 0):
        featuresStr = ", ".join(features)
        featuregroupsStrings = []
        for fg in featuregroups_version_dict:
            featuregroupsStrings.append(fg + "_" + str(featuregroups_version_dict[fg]))
        featuregroupssStr = ", ".join(featuregroupsStrings)
        return spark.sql("SELECT " + featuresStr + " FROM " + featuregroupssStr)
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
    tbl_name = featuregroup + "_" + str(featuregroup_version)
    if(mode == "append"):
        emptyBool = spark.sql("SELECT * FROM " + tbl_name + " LIMIT 1").rdd.isEmpty()
        if(emptyBool):
            mode = "overwrite" # spark complains if it tries to append to empty table because it cannot infer file-format
    sparkDF.write.format("ORC").mode(mode).saveAsTable(tbl_name)
