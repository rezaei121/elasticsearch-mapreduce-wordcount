Introduction
============

elasticsearch Map/Reduce integration (word count project)

Usage
-----

import HDFS file to elasticsearch
##### Command:
```shell
hadoop jar elasticsearch-mapreduce-wordcount.jar import_to_es <resource patch>
```

export elasticsearch index to HDFS file
##### Command:
```shell
hadoop jar elasticsearch-mapreduce-wordcount.jar export_to_hdfs <output patch>
```

License
-------
elasticsearch-mapreduce-wordcount is an open source project by Ehsan Rezaei(http://www.developit.ir) that is licensed under GPL-3.0.
