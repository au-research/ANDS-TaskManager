ANDS-TaskManager
==============

The ANDS Task Manager is a helper that runs in the background, executing tasks so that the harvesting / indexing process is improved

Running the Harvester as a Linux service
----------------------------------------

The file `ands-taskprocessor` is a System V init script to be copied into
`/etc/init.d`.
We used `ands-taskmanager` as the name of the script instead of `ands-taskprocessor`

```
cp ands-taskprocessor /etc/init.d/ands-taskmanager
chmod 755 /etc/init.d/ands-taskmanager
chkconfig --add ands-taskmanager
chkconfig ands-taskmanager on
service ands-taskmanager start
```

The Task Manager will start up, and it will be started at each boot time.
The script supports `start`, `stop`, and `status` commands.