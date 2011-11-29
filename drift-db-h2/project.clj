(defproject org.drift-db/drift-db-h2 "1.0.5-SNAPSHOT"
  :description "This is the h2 implementation of the drift-db protocol."
  :dependencies [[clojure-tools "1.1.0-SNAPSHOT"]
                 [com.h2database/h2 "1.3.162"]
                 [org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.1.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.drift-db/drift-db "1.0.5-SNAPSHOT"]])