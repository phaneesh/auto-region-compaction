# HBase Region Compaction [![Travis build status](https://travis-ci.org/phaneesh/revolver.svg?branch=master)](https://travis-ci.org/phaneesh/auto-region-compaction)

Auto compact regions to maintain file locality. Optionally send slack alert

## Features
* Compact regions which have low file locality (Configurable threshold)
* Send slack alert (Optional)
* Configurable table list 

## Usage
java -jar auto-region-compaction.jar --help 
 
### Build instructions
  - Clone the source:

        git clone github.com/phaneesh/auto-region-compaction

  - Build

        mvn package

LICENSE
-------

Copyright 2016 Phaneesh Nagaraja <phaneesh.n@gmail.com>.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.