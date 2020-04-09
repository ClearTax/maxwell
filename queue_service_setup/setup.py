import logging
import subprocess
import sys
import time

import psutil
import pymysql

from setup_config import *

log = logging.getLogger(__name__)

MVN_CLEAN_PACKAGE_CMD = ['mvn', 'clean', 'package', '-DskipTests']

MAXWELL_REPLICATION_PRIVILEGES_QUERIES = (
    "CREATE USER {user} IDENTIFIED BY '{password}';",
    "GRANT ALL ON maxwell.* TO {user};",
    "GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO {user};",
)


def _setup_logging():
    global log
    log.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.DEBUG)
    log.addHandler(handler)


def _setup_maven_project():
    output = subprocess.run(MVN_CLEAN_PACKAGE_CMD, stdout=sys.stdout,
                            cwd='../')
    """Install maven dependencies for daemon"""

    if output.returncode != 0:
        raise Exception("Maven packages were not installed properly.")
    log.info("Packages installed successfully!")
    return


def _setup_maxwell_replication_privileges():
    """This creates maxwell user and sets up replication privileges
    for the databases."""
    # Create Maxwell User on MySQL
    mysql_conn = pymysql.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                 host=MYSQL_HOST, autocommit=True)

    # Add Replication Privileges to Maxwell
    with mysql_conn.cursor() as cursor:
        for query in MAXWELL_REPLICATION_PRIVILEGES_QUERIES:
            rendered_query = query.format(user=MAXWELL_USER,
                                          password=MAXWELL_PASSWORD)
            log.info("Running Query: %s", rendered_query)
            cursor.execute(rendered_query)
    mysql_conn.close()
    log.info("Created User and Granted Privileges!")


def _terminate_maxwell_process(pid):
    parent_process = psutil.Process(pid)

    # Killing children processes
    for child_process in parent_process.children(recursive=True):
        child_process.terminate()
        child_process.wait()
    # Terminating parent process.
    parent_process.terminate()
    parent_process.wait()


def start_maxwell():
    run = [
        '../bin/maxwell',
        '--user=maxwell'.format(MAXWELL_USER),
        '--password={}'.format(MAXWELL_PASSWORD),
        '--host=localhost',
        '--config=config.properties',
        '--replication_host={}'.format(MYSQL_HOST),
        '--replication_user={}'.format(MAXWELL_USER),
        '--replication_password={}'.format(MAXWELL_PASSWORD),
        '--replication_port={}'.format(MYSQL_PORT)
    ]

    if MYSQL_SCHEMA_DATABASE is not None:
        run.append('--schema_database='.format(MYSQL_SCHEMA_DATABASE))

    log.info('Maxwell run command: %s', ' '.join(run))
    process = subprocess.Popen(run, stdout=sys.stdout)

    try:
        while True:
            return_code = process.poll()
            if return_code is not None:
                return
            time.sleep(1)
    except KeyboardInterrupt:
        # log.info("Killing Maxwell Process..")
        _terminate_maxwell_process(process.pid)


if __name__ == '__main__':
    _setup_logging()
    # _setup_maven_project()
    # _setup_maxwell_replication_privileges()
    start_maxwell()
