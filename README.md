# cs505_project


## kick off orientdb

open both port 2424 and 2480 from openstack dashboard and execute following in cmd to kick off docer instance of orientdb
`sudo docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=password orientdb:3.0.0`

## Build and execute the jar file using follwing command
bash build.sh


