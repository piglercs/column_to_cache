# MemSQL cacher

Using this tool MemSQL database table columns can be moved into or removed from cache. It can also provide information about what percentage of a table is in cache.

## Getting Started


### Prerequisites

First of all, vmtouch must be installed on each MemSQL leaf node for using MemSQL cacher (https://hoytech.com/vmtouch/).

For maintaining SSH connection paramiko is also a must.

```
pip install paramiko
```

### Configuring

MemSQL cacher configuration can be set in the config file as the following format shows:

```
SSH_COMMAND_TIMEOUT=60
SSH_USER=username
SSH_KEY=/path/to/ssh-key/key.pem
AGGREGATOR=memsql_aggregator
PORT=3306
DB_USER=dbuser
DB_PWD=dbpwd
MEMSQL_PATH=/var/lib/memsql/
VMTOUCH_PATH=/path/to/vmtouch
THREADS_PER_HOST=4
```

## Running

There are three different scripts ``cache_info.py``, ``move_to_cache.py`` and ``remove_from_cache.py``. Each needs two runtime arguments, the first is the table name and the second one is the name of the column.

````
python move_to_cache.py sld_gl_jorunals je_lines_attribute5
````