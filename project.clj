(defproject org.drift-db/drift-db-root "1.1.5-SNAPSHOT"
  :description "This is the root project for drift-db"
  
  ; To run use 'lein sub <task>'
  
  :sub ["drift-db"
        "drift-db-h2"
        "drift-db-mysql"
        "drift-db-postgresql"])
