from airflow.contrib.hooks.ssh_hook import SSHHook

# Yes, this is ugly :)

def spark_cmd(jar=None, main_class=None, egg=None, py_file=None, args=''):
    if py_file and main_class:
        raise ValueError('cannot provide both a py_file and a main_class')
    if not py_file and not main_class:
        raise ValueError('must provide a py_file or a main_class')
    if main_class and not jar:
        raise ValueError('must provide a jar when providing a main_class')

    cmd = """sudo su \
$SPARK_HOME/bin/spark-submit
"""
    if main_class:
        cmd += '  --class {}'.format(main_class)
    if egg:
        cmd += ' --py-files {}'.format(egg)
    if jar:
        cmd += ' {}'.format(jar)
    if py_file:
        cmd += ' {}'.format(py_file)
    if len(args) > 0:
        cmd += ' {}'.format(args)

    return cmd


ssh_hook = SSHHook(conn_id='spark_edgenode_ssh')
