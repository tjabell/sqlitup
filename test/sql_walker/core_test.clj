(ns sql-walker.core-test
  (:require [clojure.test :refer :all]
            [sql-walker.core :refer :all]))

(deftest addition
  (is (= 1 1)))

(def c '({:data_type "int",
           :column_name "ManufacturerId",
           :numeric_scale nil,
           :column_default nil}))

(deftest making-up-data
  (testing "Generating dummy data for a column"
    (is (= "int" (get-dummy-data (first c)))))
  (testing "Generate dummy data for table columns"
    (is (= '("int") (generate-data c)))))



