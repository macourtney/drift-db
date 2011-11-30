(defproject org.drift-db/drift-db-mysql "1.0.7-SNAPSHOT"
  :description "This is the mysql implementation of the drift-db protocol."
  :dependencies [[clojure-tools "1.1.0"]
                 [mysql/mysql-connector-java "5.1.18"]
                 [org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.1.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.drift-db/drift-db "1.0.7-SNAPSHOT"]])