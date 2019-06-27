# bagit-provider-plugin

This project contains a KIT DM 2.0 collection download provider plugin offering the download of BagIt archives compliant to the RDA RDRIWG recommendations (http://dx.doi.org/10.15497/RDA00025).

## How to build and install

In order to build this plugin you'll need:

* Java SE Development Kit 8 or higher

After obtaining the sources change to the folder where the sources are located perform the following steps:

```
user@localhost:/home/user/bagit-provider-plugin$ ./gradlew build
> Configure project :
<-------------> 0% EXECUTING [0s]
[...]
user@localhost:/home/user/bagit-provider-plugin$
```

After building the plugin, you'll find a file named 'bagit-provider-plugin.jar' at 'build/libs/'. This file has to be copied to 
your KIT DM 2.0 location into the 'lib' folder containing external libraries.

Now you can start your KIT DM 2.0 instance following the procedure decribed under [Enhanced Startup](https://git.scc.kit.edu/kitdatamanager/2.0/base-repo#enhanced-startup).
The plugin will be automatically detected and will be available after startup.

## How to use

Downloading content in a BagIt package can be done by accessing a virtual folder of a DataResource and providing 'application/vnd.datamanager.bagit+zip' in the 'Accept'
header of the HTTP request. Please also refer to the KIT DM 2.0 documentation available at http://localhost:8090/static/docs/documentation.html in section 'Downloading Data from a Data Resource'. 
You may have to change the port according to your local setup.

## License

The KIT Data Manager is licensed under the Apache License, Version 2.0.


