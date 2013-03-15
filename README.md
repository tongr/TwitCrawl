# TwitCrawl - Twitter stream crawling

Prototype for crawling links and other data from tweets (i.e., provided via Twitter streaming API)


## Uses
This prototype package supports different utilities for the extraction of tweet information.


## Requirements
* Java 1.6 - http://www.java.com/de/
* Maven 2.2.1 - http://maven.apache.org/

## Dependencies
* JUnit 4 - http://junit.org/
* mongo-java-driver 2.10.1 - https://github.com/mongodb/mongo-java-driver (see also http://www.mongodb.org/)
* jsoup 1.7.2 - http://jsoup.org/

## How to
1. mvn install
2. opt: *mvn eclipse:eclipse*
3. configure MongoDB account in src/main/resources/mongodb.private.conf (see also src/main/resources/mongodb.example.conf)


## License
This software is licensed under the Apache 2 license, quoted below.

Copyright 2013 Hasso Plattner Institute for Software Systems Engineering - http://www.hpi-web.de/

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.