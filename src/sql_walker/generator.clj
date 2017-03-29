(ns sql-walker.generator)

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
