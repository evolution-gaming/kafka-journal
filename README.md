# kafka-journal [![Build Status](https://travis-ci.org/evolution-gaming/kafka-journal.svg)](https://travis-ci.org/evolution-gaming/kafka-journal) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/kafka-journal/badge.svg)](https://coveralls.io/r/evolution-gaming/kafka-journal) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/fab03059b5f94fa5b1e7ad7bddfe8b07)](https://www.codacy.com/app/evolution-gaming/kafka-journal?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/kafka-journal&amp;utm_campaign=Badge_Grade) [ ![version](https://api.bintray.com/packages/evolutiongaming/maven/kafka-journal/images/download.svg) ](https://bintray.com/evolutiongaming/maven/kafka-journal/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

Kafka journal implementation will also need some additional storage to overcome kafka `retention policy` and prevent the full topic scan in some corner cases. 
But It has relaxed requirements for writes.



## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "kafka-journal" % "0.0.1"
```