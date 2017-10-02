# dslink-java-template

* Version: 0.0.0
* Java - version 1.6 and up.
* [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)


## Overview

This project can be used to create the boilerplate for a new Java DSLink.  In a nutshell, copy it, 
change org.iot.dsa.dlink.template to your package structure and globally replace 
'dslink-java-template' with your link name.

If you are not familiar with DSA, an overview can be found at
[here](http://iot-dsa.org/get-started/how-dsa-works).

This link was built using the Java DSLink SDK which can be found
[here](https://github.com/iot-dsa-v2/sdk-dslink-java).

## Files Requiring Modification

1. build.gradle
    - Group - Your organization's identifier.
    - Version - major.minor.patch[-pre-release]
    - mainClassName - You will probably use the main class, but your package will be 
      different.
    - dependencies - Replace the sdk-dslink-java project dependencies with the commented out
      versions on the following line.
2. dslink.json
    - Change everything that isn't (above) configs.
    - Change the rootType config to the fully qualified class name of your root node type.
3. README.md
    - Change the title link name.
    - Maintain the version number.
    - Change the license if necessary.
    - Provide some sort of Overview.
    - Maintain a version history.
4. Main.java
    - If you reuse this class, be sure to change the directory structure to match your package.

## Link Development

Please see the [developer guide](https://iot-dsa-v2.github.io/sdk-dslink-java/DeveloperGuide) and 
the [Javadoc](https://iot-dsa-v2.github.io/sdk-dslink-java/javadoc/) for the core SDK.

## Acknowledgements

Attributions belong here.

## History

* Version 0.0.0
  - Hello World


