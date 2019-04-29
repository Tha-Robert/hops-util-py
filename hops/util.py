"""

Miscellaneous utility functions for user applications.

"""

import os
import signal
from ctypes import cdll
import itertools
import socket
import json
import base64
from datetime import datetime
import time
from hops import hdfs
from hops import version
from pyspark.sql import SparkSession
from hops import constants
import ssl
import threading
import re

#! Needed for hops library backwards compatability
try:
    import requests
except:
    pass
import pydoop.hdfs

try:
    import tensorflow
except:
    pass

try:
    import http.client as http
except ImportError:
    import httplib as http


def _get_elastic_endpoint():
    """

    Returns:

    """
    elastic_endpoint = os.environ[constants.ENV_VARIABLES.ELASTIC_ENDPOINT_ENV_VAR]
    host, port = elastic_endpoint.split(':')
    return host + ':' + port

elastic_endpoint = None
try:
    elastic_endpoint = _get_elastic_endpoint()
except:
    pass


def _get_hopsworks_rest_endpoint():
    """

    Returns:

    """
    elastic_endpoint = os.environ[constants.ENV_VARIABLES.REST_ENDPOINT_END_VAR]
    return elastic_endpoint

hopsworks_endpoint = None
try:
    hopsworks_endpoint = _get_hopsworks_rest_endpoint()
except:
    pass

def _find_in_path(path, file):
    """

    Args:
        :path:
        :file:

    Returns:

    """
    for p in path.split(os.pathsep):
        candidate = os.path.join(p, file)
        if (os.path.exists(os.path.join(p, file))):
            return candidate
    return False

def _find_tensorboard():
    """

    Returns:
         tb_path
    """
    pypath = os.getenv("PYSPARK_PYTHON")
    pydir = os.path.dirname(pypath)
    search_path = os.pathsep.join([pydir, os.environ[constants.ENV_VARIABLES.PATH_ENV_VAR], os.environ[constants.ENV_VARIABLES.PYTHONPATH_ENV_VAR]])
    tb_path = _find_in_path(search_path, 'tensorboard')
    if not tb_path:
        raise Exception("Unable to find 'tensorboard' in: {}".format(search_path))
    return tb_path

def _on_executor_exit(signame):
    """
    Return a function to be run in a child process which will trigger
    SIGNAME to be sent when the parent process dies

    Args:
        :signame:

    Returns:
        set_parent_exit_signal
    """
    signum = getattr(signal, signame)
    def set_parent_exit_signal():
        # http://linux.die.net/man/2/prctl

        PR_SET_PDEATHSIG = 1
        result = cdll['libc.so.6'].prctl(PR_SET_PDEATHSIG, signum)
        if result != 0:
            raise Exception('prctl failed with error code %s' % result)
    return set_parent_exit_signal

def _get_host_port_pair():
    """
    Removes "http or https" from the rest endpoint and returns a list
    [endpoint, port], where endpoint is on the format /path.. without http://

    Returns:
        a list [endpoint, port]
    """
    endpoint = _get_hopsworks_rest_endpoint()
    if 'http' in endpoint:
        last_index = endpoint.rfind('/')
        endpoint = endpoint[last_index + 1:]
    host_port_pair = endpoint.split(':')
    return host_port_pair

def _get_http_connection(https=False):
    """
    Opens a HTTP(S) connection to Hopsworks

    Args:
        https: boolean flag whether to use Secure HTTP or regular HTTP

    Returns:
        HTTP(S)Connection
    """
    host_port_pair = _get_host_port_pair()
    if (https):
        PROTOCOL = ssl.PROTOCOL_TLSv1_2
        ssl_context = ssl.SSLContext(PROTOCOL)
        connection = http.HTTPSConnection(str(host_port_pair[0]), int(host_port_pair[1]), context = ssl_context)
    else:
        connection = http.HTTPConnection(str(host_port_pair[0]), int(host_port_pair[1]))
    return connection

def num_executors():
    """
    Get the number of executors configured for Jupyter

    Returns:
        Number of configured executors for Jupyter
    """
    sc = _find_spark().sparkContext
    return int(sc._conf.get("spark.executor.instances"))

def num_param_servers():
    """
    Get the number of parameter servers configured for Jupyter

    Returns:
        Number of configured parameter servers for Jupyter
    """
    sc = _find_spark().sparkContext
    return int(sc._conf.get("spark.tensorflow.num.ps"))

def grid_params(dict):
    """
    Generate all possible combinations (cartesian product) of the hyperparameter values

    Args:
        :dict:

    Returns:
        A new dictionary with a grid of all the possible hyperparameter combinations
    """
    keys = list(dict.keys())
    val_arr = []
    for key in keys:
        val_arr.append(dict[key])

    permutations = list(itertools.product(*val_arr))

    args_dict = {}
    slice_index = 0
    for key in keys:
        args_arr = []
        for val in list(zip(*permutations))[slice_index]:
            args_arr.append(val)
        slice_index += 1
        args_dict[key] = args_arr
    return args_dict

def _get_ip_address():
    """
    Simple utility to get host IP address

    Returns:
        x
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

def _time_diff(task_start, task_end):
    """
    Args:
        :task_start:
        :tast_end:

    Returns:

    """
    time_diff = task_end - task_start

    seconds = time_diff.seconds

    if seconds < 60:
        return str(int(seconds)) + ' seconds'
    elif seconds == 60 or seconds <= 3600:
        minutes = float(seconds) / 60.0
        return str(int(minutes)) + ' minutes, ' + str((int(seconds) % 60)) + ' seconds'
    elif seconds > 3600:
        hours = float(seconds) / 3600.0
        minutes = (hours % 1) * 60
        return str(int(hours)) + ' hours, ' + str(int(minutes)) + ' minutes'
    else:
        return 'unknown time'

def _put_elastic(project, appid, elastic_id, json_data):
    """
    Args:
        :project:
        :appid:
        :elastic_id:
        :json_data:

    Returns:

    """
    if not elastic_endpoint:
        return
    headers = {'Content-type': 'application/json'}
    session = requests.Session()

    retries = 3
    while retries > 0:
        resp = session.put("http://" + elastic_endpoint + "/" +  project + "_experiments/experiments/" + appid + "_" + str(elastic_id), data=json_data, headers=headers, verify=False)
        if resp.status_code == 200:
            return
        else:
            time.sleep(20)
            retries = retries - 1

    raise RuntimeError("Failed to publish experiment json file to Elastic, it is possible Elastic is experiencing problems. "
                       "Please contact an administrator.")



def _populate_experiment(sc, model_name, module, function, logdir, hyperparameter_space, versioned_resources, description):
    """
    Args:
         :sc:
         :model_name:
         :module:
         :function:
         :logdir:
         :hyperparameter_space:
         :versioned_resources:
         :description:

    Returns:

    """
    user = None
    if constants.ENV_VARIABLES.HOPSWORKS_USER_ENV_VAR in os.environ:
        user = os.environ[constants.ENV_VARIABLES.HOPSWORKS_USER_ENV_VAR]
    return json.dumps({'project': hdfs.project_name(),
                       'user': user,
                       'name': model_name,
                       'module': module,
                       'function': function,
                       'status':'RUNNING',
                       'app_id': sc.applicationId,
                       'start': datetime.now().isoformat(),
                       'memory_per_executor': str(sc._conf.get("spark.executor.memory")),
                       'gpus_per_executor': str(sc._conf.get("spark.executor.gpus")),
                       'executors': str(sc._conf.get("spark.executor.instances")),
                       'logdir': logdir,
                       'hyperparameter_space': hyperparameter_space,
                       'versioned_resources': versioned_resources,
                       'description': description})

def _finalize_experiment(experiment_json, hyperparameter, metric):
    """
    Args:
        :experiment_json:
        :hyperparameter:
        :metric:

    Returns:

    """
    experiment_json = json.loads(experiment_json)
    experiment_json['metric'] = metric
    experiment_json['hyperparameter'] = hyperparameter
    experiment_json['finished'] = datetime.now().isoformat()
    experiment_json['status'] = "SUCCEEDED"
    experiment_json = _add_version(experiment_json)

    return json.dumps(experiment_json)

def _add_version(experiment_json):
    experiment_json['spark'] = os.environ['SPARK_VERSION']

    try:
        experiment_json['tensorflow'] = tensorflow.__version__
    except:
        experiment_json['tensorflow'] = os.environ[constants.ENV_VARIABLES.TENSORFLOW_VERSION_ENV_VAR]

    experiment_json['hops_py'] = version.__version__
    experiment_json['hops'] = os.environ[constants.ENV_VARIABLES.HADOOP_VERSION_ENV_VAR]
    experiment_json['hopsworks'] = os.environ[constants.ENV_VARIABLES.HOPSWORKS_VERSION_ENV_VAR]
    experiment_json['cuda'] = os.environ[constants.ENV_VARIABLES.CUDA_VERSION_ENV_VAR]
    experiment_json['kafka'] = os.environ[constants.ENV_VARIABLES.KAFKA_VERSION_ENV_VAR]
    return experiment_json

def _store_local_tensorboard(local_tb, hdfs_exec_logdir):
    """

    Args:
        :local_tb:
        :hdfs_exec_logdir:

    Returns:

    """
    tb_contents = os.listdir(local_tb)
    for entry in tb_contents:
        pydoop.hdfs.put(local_tb + '/' + entry, hdfs_exec_logdir)

def _version_resources(versioned_resources, rundir):
    """

    Args:
        versioned_resources:
        rundir:

    Returns:

    """
    if not versioned_resources:
        return None
    pyhdfs_handle = hdfs.get()
    pyhdfs_handle.create_directory(rundir)
    endpoint_prefix = hdfs.project_path()
    versioned_paths = []
    for hdfs_resource in versioned_resources:
        if pydoop.hdfs.path.exists(hdfs_resource):
            log("Versoning resource '%s'" % hdfs_resource)
            pyhdfs_handle.copy(hdfs_resource, pyhdfs_handle, rundir)
            path, filename = os.path.split(hdfs_resource)
            versioned_paths.append(rundir.replace(endpoint_prefix, '') + '/' + filename)
        else:
            log("Resource not found '%s'" % hdfs_resource, level='warning')
            #raise Exception('Could not find resource in specified path: ' + hdfs_resource)

    return ', '.join(versioned_paths)

def _convert_to_dict(best_param):
    """

    Args:
        best_param:

    Returns:

    """
    best_param_dict={}
    for hp in best_param:
        hp = hp.split('=')
        best_param_dict[hp[0]] = hp[1]

    return best_param_dict

def _get_logger(name):
        import logging
        import logstash
        import socket
        import re

        logger = logging.getLogger(name)
        if len(logger.handlers) > 0 or hasattr(logger, "_no_logging"):
            return logger

        # Get from user YARN_IDENT_STRING
        # Figure out where the spark log4j conf is to get info
        log4j_conf = os.path.join(os.environ["HADOOP_LOG_DIR"], "..", "..",
                                  "spark", "conf", "log4j.properties")
        log4j_content = open(log4j_conf, "rb").read().decode("UTF-8")
        # We use port 5000 not 3456
        #port_re = re.search("^log4j.appender.tcp.Port=(\d+)",log4j_content, re.M)
        host_re = re.search("^log4j.appender.tcp.RemoteHost=([^ \t]+)[ \t]*$", log4j_content, re.M)
        # log4j.appender.tcp.Port=3456
        # log4j.appender.tcp.RemoteHost=10.0.0.9

        if not host_re:
            result = 1
        else:
            host = socket.gethostbyname(host_re.group(1))
            #port = int(port_re.group(1))
            #5000
            port = 5000
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((host,port))

        if result == 0:
            # init logger
            logger.setLevel(logging.INFO)
            # TCP
            logger.addHandler(logstash.TCPLogstashHandler(host, port, version=1))
            # UDP
            # logger.addHandler(logstash.LogstashHandler(host, port, version=1))
            log('Logstash logger started!', logger=logger)
            #print("hops-util: Logstash logger started!")
        else:
            print("hops-util: No logstash logger found")
            logger._no_logging = True

        return logger


def log(line, level='info', logger=None, thread="default"):
    # For logging to work you need to add this to logstash and restart the service
    # input {
    #   tcp {
    #     port => 5000
    #     codec => json
    #   }
    # }
    #
    # Will do normal printing if all fails

    # add extra field to logstash message
    if logger is not None:
        mlogger = logger
    else:
        # Maybe needs to add executor here also if we have multiple
        mlogger = _get_logger("executor-logger-%s" % os.environ['CONTAINER_ID'])
        if not mlogger:
            print("Logger error returned None")
            return
        if hasattr(mlogger, "_no_logging"):
            print(line)
            return True

    if hasattr(mlogger, "_no_logging"):
        print(line)
        return True

    import time
    import datetime

    extra = {
        'application': [hdfs.project_name().lower(), "jupyter", "notebook", "executor128"],
        'timestamp'  : datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'priority'   : level
        #'thread'     : thread
    }
    getattr(mlogger, level)('%s', line, extra=extra)
    return True

def setup_bg_thread(func, name, condition=True):
    t = threading.Thread(target=func, name=name)
    if condition:
        t.start()
    return t

def teardown_bg_thread(t):
    t.do_run = False
    if t.isAlive():
        t.join()

def format_traceback(e):
    import types
    import traceback
    import sys
    from funcsigs import signature

    def log_to_str(v):
        if isinstance(v, bytes):
            return ''.join(["'", v.replace('\n', '\\n'), "'"])
        else:
            try:
                return str(v).replace('\n', '\\n')
            except:
                return '<ERROR: CANNOT PRINT>'

    frame = sys.exc_info()[2]
    formattedTb = traceback.format_tb(frame)
    exception_string = "Globals:\n"
    for k,v in list(frame.tb_frame.f_globals.items()):
        exception_string += "{:>4}{:<20}:{}{:<.100}\n".format("", k, " ", log_to_str(v))
    exception_string += "Traceback:\n"
    while frame:
        this_frame_tb = formattedTb.pop(0)
        exception_string += this_frame_tb
        call_re_search = re.search("^[\t ]+(.+)\(.*?\)$", this_frame_tb, re.M)
        co_name = frame.tb_frame.f_code.co_name
        if call_re_search and call_re_search.group(1) in list(frame.tb_frame.f_locals.keys()) + list(frame.tb_frame.f_globals.keys()):
            call_name = call_re_search.group(1)
            if call_name in frame.tb_frame.f_locals:
                if call_name != frame.tb_frame.f_locals[call_name].__name__:
                    exception_string = exception_string[:-1]
                    exception_string += " => {}{}\n".format(frame.tb_frame.f_locals[call_name].__name__, str(signature(frame.tb_frame.f_locals[call_name])))
            elif co_name in frame.tb_frame.f_globals:
                if call_name != frame.tb_frame.f_globals[call_name].__name__:
                    exception_string = exception_string[:-1]
                    exception_string += " => {}{}\n".format(frame.tb_frame.f_globals[call_name].__name__, str(signature(frame.tb_frame.f_globals[call_name])))
        exception_string += "    Locals:\n"
        for k,v in list(frame.tb_frame.f_locals.items()):
            exception_string += "{:<8}{:<20}:{}{:<.100}\n".format("", k, " ", log_to_str(v))
        frame = frame.tb_next

    exception_string += 'Exception thrown, {}: {}\n'.format(type(e), str(e))
    return exception_string

def _find_spark():
    """

    Returns:

    """
    return SparkSession.builder.getOrCreate()
