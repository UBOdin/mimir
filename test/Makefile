all: 
	@echo 'Run `make [dataset]` to get one of the test datasets.  You can get:'
	@echo 'make tpch      :  A 1GB TPC-H dataset'
	@echo 'make pdbench   :  A 1GB TPC-H worldset-decomposed dataset with errors for PDBench (http://pdbench.sourceforge.net/)'
	@echo 'make impute    :  A 1GB TPC-H SQLite database with random cells replaced by nulls'


tpch: 
	#curl -O http://odin.cse.buffalo.edu/public_data/tpch.tgz
	mkdir tpch
	tar -zxvf tpch.tgz --strip-components=1 --directory=tpch 
	rm tpch.tgz


pdbench: 
	curl -O http://odin.cse.buffalo.edu/public_data/pdbench-1g-columnar.tgz
	mkdir pdbench
	tar -zxvf pdbench-1g-columnar.tgz --strip-components=1 --directory=pdbench
	rm pdbench-1g-columnar.tgz

impute: tpch-impute-1g.db

tpch-impute-1g.db:
	curl -o tpch-impute-1g.db http://odin.cse.buffalo.edu/public_data/tpch-impute-1g.db

.PHONY: impute
