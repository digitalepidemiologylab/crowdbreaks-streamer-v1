"""
This is some hackery to set env variables in your current terminal before calling scripts.

Run this script on the commmand line using:
    ```
    eval $(python set_env.py)

    ```

Alternatively, set a shortcut:
    ```
    alias setblahblahenv="eval $(python set_env.py)"

    ```
"""


import os
import pipes

# read secrets file
secrets_file_path = os.path.join('..', 'secrets.list')
if not os.path.isfile(secrets_file_path):
    secrets_file_path = os.path.join('secrets.list')
if not os.path.isfile(secrets_file_path):
    raise Exception('secrets.list file could not be found in current or parent directory.')

f =  open(secrets_file_path, 'r')

for l in f.readlines():
    if not l.startswith('#'):
        split = l.strip().split('=')
        if len(split) == 2:
            var, value = split
            print("export {}={}".format(var, pipes.quote(str(value))))
f.close()
