# dslink-java-v2-jdbc

* Version: 1.0.0.0
* Java - version 1.7 and up.
* [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Overview

Link to connect to JDBC databases built in Java SDK v2. 
After installing and staring the link, connect to a new database sing the 
"Add DB" action on the root node. Once connected, new databases will show up as children nodes of the root node.
Use the "Query" action to run SQL Queries on the database, use "Disconnect" task to 
delete the node and disconnect from the database.

If you are not familiar with DSA, an overview can be found at
[here](http://iot-dsa.org/get-started/how-dsa-works).

This link was built using the Java DSLink SDK which can be found
[here](https://github.com/iot-dsa-v2/sdk-dslink-java).

## Link Architecture

This section outlines the hierarchy of nodes defined by this link.

- _RootNode_ - Used to add new database connections.
  - _ConnectionNode_ - Used to run SQL queries and Disconnect databases.


## Node Guide

The following section provides detailed descriptions of each node in the link as well as
descriptions of actions, values and child nodes.


### RootNode

This is the root node of the link.  It has actions for connecting to new databases.

**Actions**
- Add DB - Adds a new database

**Values**
- Docs - Link to the GitHub Documentation

**Child Nodes**
- ConnectionNode - A new connection node is create for each new database.

### ConnectionNode

Each connection node represents a new database connection.

**Actions**
- Query - Run an SQL query.
- Edit - Edit the parameters for the database. (fields left blank are not changed)
- Disconnect - Disconnect from database and delete the ConnectionNode.

**Values**
- Connection Status - Displays the status of the connection.
- DB Name - Display the name of the database node.
- Driver - Driver used to connect to the database.
- Last Fail Con - Timestamp when connection last failed
- Last Success Con - Timestamp when connection was last sucessful
- URL - URL of the database.
- User Name - User name used for this connection.

## Acknowledgements

DRIVERS

This software uses the following drivers for connecting to databases:
* [MySQL](https://dev.mysql.com/)
* [H2 Database Engine](http://www.h2database.com)
* [PostgreSQL](https://www.postgresql.org/)

SDK-DSLINK-JAVA

This software contains unmodified binary redistributions of 
[sdk-dslink-java](https://github.com/iot-dsa-v2/sdk-dslink-java), which is licensed 
and available under the Apache License 2.0. An original copy of the license agreement can be found 
at https://github.com/iot-dsa-v2/sdk-dslink-java/blob/master/LICENSE

## History

* Version 1.0.0.0
  - Basic functionality implemented
  - Add/Remove/Edit Databases
  - Run Queries
* Version 0.0.0.0
  - Initial Development