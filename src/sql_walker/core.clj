(ns sql-walker.core
  (:require [clojure.string :as s]
            [clojure.java.jdbc :as j]
            [sql-walker.config :as config]))

(def db config/db)

(defn q
  [query]
  (j/query db query))

(def query q)
(def qm
  (memoize q))
(defn s
  [t]
  (q (str "select * from " t)))

(defn ist [t]
  (def iss "INFORMATION_SCHEMA")
  (str iss "." t))

(def tc  (ist "TABLE_CONSTRAINTS"))
(def ccu (ist "CONSTRAINT_COLUMN_USAGE"))
(def rc  (ist "REFERENTIAL_CONSTRAINTS"))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(def entity-tables '("AcdhhApplicationComment" "AcdhhAZTEDP_Applications" "AcdhhContacts" "AcdhhEquipment" "AcdhhEquipmentHistory" "AcdhhLicensure_ApplicationDeficiencies" "AcdhhLicensure_ApplicationFiles" "AcdhhLicensure_Applications" "AcdhhTransactionAdditionalReasons" "AcdhhTransactions" "AcdhhVouchers"))


(def get-key-sql (str
"    SELECT"
"    FK_Table = FK.TABLE_NAME,"
"    FK_Column = CU.COLUMN_NAME,"
"    PK_Table = PK.TABLE_NAME,"
"    PK_Column = PT.COLUMN_NAME,"
"    Constraint_Name = C.CONSTRAINT_NAME"
" FROM"
"    INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C"
" INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK"
"    ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME"
" INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK"
"     ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME"
" INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU"
"     ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME"
" INNER JOIN ("
"            SELECT"
"                i1.TABLE_NAME,"
"                i2.COLUMN_NAME"
"            FROM"
"                INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1"
"            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2"
"                ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME"
"            WHERE"
"                i1.CONSTRAINT_TYPE = 'PRIMARY KEY'"
"           ) PT"
"    ON PT.TABLE_NAME = PK.TABLE_NAME"))

(defn get-constraints
  [t]
  (q (str
      get-key-sql
      " WHERE FK.TABLE_NAME = "
      t)))

(defn get-edges [t]
  (get-constraints t))

(defn bigint-generator-function [col-spec] 1)
(defn bit-generator-function [col-spec] 1)
(defn datetime-generator-function [col-spec] "date")
(defn decimal-generator-function [col-spec] "decimal")
(defn int-generator-function [col-spec] "int")
(defn nvarchar-generator-function [col-spec] "nvarchar")
(defn smallint-generator-function [col-spec] "smallint")
(defn tinyint-generator-function [col-spec] "tinyint")
(defn uniqueidentifier-generator-function [col-spec] "uniqueidentifier")
(defn varbinary-generator-function [col-spec] "varbinary")

(def column-types
  {"bigint" bigint-generator-function
   "bit" bit-generator-function
   "datetime" datetime-generator-function
   "decimal" decimal-generator-function
   "int" int-generator-function
   "nvarchar" nvarchar-generator-function
   "smallint" smallint-generator-function
   "tinyint" tinyint-generator-function
   "uniqueidentifier" uniqueidentifier-generator-function
   "varbinary" varbinary-generator-function})

(defn get-dummy-data
  "Fills a column with dummy data"
  [column]
  ((get column-types (:data_type column)) column))

(defn generate-data
  "Gets columns from a table"
  [columns]
  (->> columns
       (map get-dummy-data)))


(defn retrieve-columns
  [table]
  (query (str "select data_type, column_name, numeric_scale, column_default from information_schema.columns where table_name = '" table "'")))

(defn fill-table
  "Fills up a table with values, grabs first value of parent table or errors"
  [table]
  (->> (retrieve-columns table)
       (generate-data)))

    
;; from https://stackoverflow.com/questions/925738/how-to-find-foreign-key-dependencies-in-sql-server
;;   SELECT
;;     FK_Table = FK.TABLE_NAME,
;;     FK_Column = CU.COLUMN_NAME,
;;     PK_Table = PK.TABLE_NAME,
;;     PK_Column = PT.COLUMN_NAME,
;;     Constraint_Name = C.CONSTRAINT_NAME
;; FROM
;;     INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
;; INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
;;     ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
;; INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
;;     ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
;; INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU
;;     ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
;; INNER JOIN (
;;             SELECT
;;                 i1.TABLE_NAME,
;;                 i2.COLUMN_NAME
;;             FROM
;;                 INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
;;             INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2
;;                 ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
;;             WHERE
;;                 i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
;;            ) PT
;;     ON PT.TABLE_NAME = PK.TABLE_NAME

