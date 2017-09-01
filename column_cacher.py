import threading
import time
import pymysql
import re
from collections import defaultdict

import paramiko


class ColumnCacher:

    parameters = None

    file_paths_on_hosts = None
    clients = None

    config_file = 'config'

    function_mapper = {}

    QUERY_SEGMENTS = 'SELECT LOWER(HOST), PORT, FILE FROM ' \
                     'information_schema.columnar_segments ' \
                     'WHERE TABLE_NAME = \'{table_name}\' ' \
                     'AND COLUMN_NAME = \'{column_name}\' '

    pages_results = None

    def __enter__(self):
        return self

    def __init__(self, table_name, column_name):
        self.clients = {}
        self.pages_results = []
        self.function_mapper = {'info': self.table_cache_info_by_client,
                                'touch': self.table_cacher,
                                'evict': self.table_remove_from_cache}

        with open(self.config_file) as f:
            self.parameters = dict(map(lambda x: x.split('='),
                                       map(lambda y: y.rstrip(), f.readlines())))
        self.parameters['TABLE_NAME'] = table_name
        self.parameters['COLUMN_NAME'] = column_name
        self.init_clients_and_file_paths()

    def chunk_list(self,seq, num):
        avg = len(seq) / float(num)
        out = []
        last = 0.0

        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg

        return out

    def get_paths_from_information_schema(self):
        connection = pymysql.connect(host=self.parameters['AGGREGATOR'],
                                     port=int(self.parameters['PORT']),
                                     user=self.parameters['DB_USER'],
                                     passwd=self.parameters['DB_PWD'])
        cursor = connection.cursor()
        cursor.execute(self.QUERY_SEGMENTS.format(table_name=self.parameters['TABLE_NAME'],
                                             column_name=self.parameters['COLUMN_NAME']))

        result = cursor.fetchall()
        connection.close()

        paths_from_schema = defaultdict(list)

        for host, port, path in result:
            paths_from_schema[host].append((port, path))

        return paths_from_schema

    def init_clients_and_file_paths(self):
        self.file_paths_on_hosts = self.get_paths_from_information_schema()

        for hostname in self.file_paths_on_hosts.keys():
            client = self.init_ssh_connection(hostname, self.parameters['SSH_USER'],
                                              self.parameters['SSH_KEY'])
            self.clients[hostname] = client
            # fix filepaths
            dir_map = {
                '3306': self.get_leaf_dir(client, 3306).decode('utf-8').rstrip(),
                '3307': self.get_leaf_dir(client, 3307).decode('utf-8').rstrip()
                }

            self.file_paths_on_hosts[hostname] = \
                list(map(lambda x: self.parameters['MEMSQL_PATH'] +
                                   dir_map[str(x[0])] + '/data/' + x[1],
                         self.file_paths_on_hosts[hostname]))

    def init_ssh_connection(self,hostname, username, key_path):
        k = paramiko.RSAKey.from_private_key_file(key_path)
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print("Connecting to %s ..." % hostname)
        c.connect(hostname=hostname, username=username, pkey=k)
        print("... connected.")
        return c

    def ssh_run_command(self,client, command):
        endtime = time.time() + int(self.parameters['SSH_COMMAND_TIMEOUT'])
        stdin, stdout, stderr = client.exec_command(command)
        while not stdout.channel.eof_received:
            time.sleep(1)
            if time.time() > endtime:
                stdout.channel.close()
                break
        out_result = stdout.read()
        err_result = stderr.read()

        return out_result, err_result

    def get_leaf_dir(self, client, leaf):
        command = 'ls /var/lib/memsql | grep %d' % leaf
        leaf_out, leaf_err = self.ssh_run_command(client, command)
        return leaf_out

    def _get_cache_info_for_file(self,client, f):
        command = 'sudo %s -v %s' %(self.parameters['VMTOUCH_PATH'], f)
        leaf_out, leaf_err = self.ssh_run_command(client, command)
        return leaf_out

    def _move_into_cache(self, client, f):
        command = 'sudo %s -vt %s' %(self.parameters['VMTOUCH_PATH'], f)
        leaf_out, leaf_err = self.ssh_run_command(client, command)
        return leaf_out

    def _remove_from_cache(self, client, f):
        command = 'sudo %s -ve %s' %(self.parameters['VMTOUCH_PATH'], f)
        leaf_out, leaf_err = self.ssh_run_command(client, command)
        return leaf_out

    def get_resident_pages(self,result):
        return map(lambda x: int(x), re.search('Resident Pages: (\d+)/(\d+)',
                                               result.decode('utf-8')).groups())

    def table_cache_info_by_client(self, client, hostname, threadname, file_list):
        list_size = len(file_list)
        for idx, f in enumerate(file_list):
            result = self.get_resident_pages(self._get_cache_info_for_file(client, f))
            self.pages_results.append(result)
            print('%s / %s is at %.2f%%' % (hostname, threadname, ((idx / list_size) * 100)))

    def run_threads(self, function):
        threads = []
        for host in self.clients.keys():
            file_groups = self.chunk_list(self.file_paths_on_hosts[host],
                                          self.parameters['THREADS_PER_HOST'])

            for i in range(int(self.parameters['THREADS_PER_HOST'])):
                threads.append(threading.Thread(target=self.function_mapper[function],
                                                args=(self.clients[host], host,
                                                      'Thread #%d' % i, file_groups.pop())))

        _ = [t.start() for t in threads]
        _ = [t.join() for t in threads]

    def cache_info_by_column(self):
        start_time = time.time()

        self.run_threads('info')

        end_time = time.time() - start_time
        print(end_time)
        try:
            cached, all = zip(*self.pages_results)
            print('%.2f%% of the table column is cached' % (sum(cached) / sum(all) * 100))
        except:
            pass

    def touch_column(self):
        start_time = time.time()

        self.run_threads('touch')

        end_time = time.time() - start_time
        print(end_time)

    def evict_column(self):
        start_time = time.time()

        self.run_threads('evict')

        end_time = time.time() - start_time
        print(end_time)

    def table_cacher(self, client, hostname, threadname, file_list):
        list_size = len(file_list)
        for idx, f in enumerate(file_list):
            self._move_into_cache(client, f)
            print('%s / %s is at %.2f%% caching table %s' % (hostname, threadname,
                                                           ((idx / list_size) * 100),
                                                             self.parameters['TABLE_NAME']))

    def table_remove_from_cache(self, client, hostname, threadname, file_list):
        list_size = len(file_list)
        for idx, f in enumerate(file_list):
            self._remove_from_cache(client, f)
            print('%s / %s is at %.2f%% removing table %s from cache' % (hostname, threadname,
                                                                       (idx / list_size) * 100,
                                                                    self.parameters['TABLE_NAME']))

    def print_cache_info(self, result, hostname='unkown', dir='unkown'):
        print('---------------------------------------------------')
        print(hostname + ' : ' + dir)
        try:
            print(re.search('Resident Pages:\s+\d+/\d+\s+\d+G/\d+G\s+\d+.\d+%',
                            result.decode('utf-8')).group(0))
        except AttributeError:
            print(result)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for c in self.clients.values():
            c.close()