ANDS-TaskManager
==============

The ANDS Task Manager is a helper that runs in the background, executing tasks so that the harvesting / indexing process is improved

Running the Harvester as a Linux service
----------------------------------------

The file `ands-taskprocessor` is a System V init script to be copied into
`/etc/init.d`. Once copied into place, run:

```
chmod 755 /etc/init.d/ands-taskprocessor
chkconfig --add ands-taskprocessor
chkconfig ands-taskprocessor on
service ands-taskprocessor start
```

The Task Manager will start up, and it will be started at each boot time.
The script supports `start`, `stop`, and `status` commands.