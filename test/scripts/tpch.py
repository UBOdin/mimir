#!/usr/bin/python

import os
import subprocess
import sys
import glob
import sqlite3
import math
import argparse
import random

scripts = os.path.dirname(os.path.realpath(__file__))
mimirhome = os.path.abspath(os.path.join(scripts, "..", ".."))
schemafile = os.path.join(mimirhome, "test", "tpch_queries", "tpch_schema.sql")
databasesfolder = "databases"



nullable = { 													\
	"SUPPLIER": 	["NATIONKEY"], 								\
	"PARTSUPP": 	["PARTKEY", "SUPPKEY"], 					\
	"CUSTOMER": 	["NATIONKEY"], 								\
	"LINEITEM": 	["ORDERKEY", "PARTKEY", "SUPPKEY"], 		\
	"ORDERS": 		["CUSTKEY"], 								\
	"NATION": 		["REGIONKEY"], 								\
	}


def preparedbgen():
	if not os.path.exists("tpch-dbgen"):
		print("tpch-dbgen not found, cloning from https://github.com/Legacy25/tpch-dbgen")
		try:
			subprocess.check_call(["git", "clone", "https://github.com/Legacy25/tpch-dbgen"])
		except subprocess.CalledProcessError:
			print("Failed to clone, exiting...")
			sys.exit(0)

		os.chdir("tpch-dbgen")
		print("Making")
		try:
			subprocess.check_call(["make"])
		except subprocess.CalledProcessError:
			print("Failed to make, exiting...")
			sys.exit(0)
		print("Make successful")
		os.chdir("..")
	else:
		print("tpch-dbgen ready")

def generatetables(scale):
	preparedbgen()
	os.chdir("tpch-dbgen")
	print("Deleting all .tbl files")
	files = glob.glob("*.tbl")
	for f in files:
		print("Deleting {0}".format(f))
		os.remove(f)

	print("Generating tables with scale-factor {0}, make sure there is enough disk space for this operation".format(scale))
	try:
		subprocess.check_call(["./dbgen", "-s", "{0}".format(scale)])

		size = 0
		files = glob.glob("*.tbl")
		for f in files:
			size += os.stat(f).st_size

		print("Generate successful, size: {0} MB".format(size/(1024*1024)))

	except subprocess.CalledProcessError:
		print("Generate failed, exiting...")
		sys.exit(0)

	os.chdir(mimirhome)


def sqliteload(db, noise):

	print("\n\nSqlite load, initializing db with name {0} and schema file {1}\n\n".format(db, schemafile))
	dbpath = os.path.join(databasesfolder, db)

	if os.path.exists(dbpath):
		print("Removing existing database file {0}".format(db))
		os.remove(dbpath)

	try:
		subprocess.check_call(["sqlite3", "-init", schemafile, dbpath, ".quit"])
	except subprocess.CalledProcessError:
		print("Initiailization failed, exiting...")
		sys.exit(0)

	files = glob.glob(os.path.join("tpch-dbgen", "*.tbl"))
	for f in files:
		outfile = f.split(".tbl")[0]+".stripped"
		outfilefd = open(outfile, 'w')
		with open(f, 'r') as fd:
			for line in fd:
				outfilefd.write(line[:-2]+"\n")

		outfilefd.close()

		print("Loading data for file {0}".format(outfile))
		try:
			subprocess.check_call(["sqlite3", dbpath, ".import {0} {1}".format(outfile, os.path.basename(outfile).split(".stripped")[0])])
		except subprocess.CalledProcessError:
			print("Import failed, exiting...")
			sys.exit(0)

	files = glob.glob(os.path.join("tpch-dbggen", "*.stripped"))
	for f in files:
		os.remove(f)

	print("Load finished, initializing MIMIR_LENSES...")
	
	lensesinit = """
			CREATE TABLE MIMIR_LENSES(
				name varchar(30), 
				query text, 
				lens_type varchar(30),
				parameters text,
				PRIMARY KEY(name)
			)
		"""

	# Create database connection
	conn = sqlite3.connect(dbpath)
	c = conn.cursor()
	
	# Create tables
	c.execute(lensesinit)

	print("Lens table initialized, scattering nulls")

	for table in nullable:
		print("Table {0}".format(table))
		rowcount = c.execute("SELECT COUNT(*) FROM {0}".format(table)).fetchone()[0]
		numnulls = int(math.ceil(noise / 100 * rowcount))
		print("Rowcount: {0} Nulls: {1}".format(rowcount, numnulls))
		for column in nullable[table]:
			for i in random.sample(xrange(rowcount), numnulls):
				c.execute("UPDATE {0} SET {1} = NULL WHERE ROWID = {2}".format(table, column, i))

	conn.commit()






def oracleload(db, noise):
	print("Oracle load")
	# TODO


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description= """
		Generate a tpch database with given size
		"""
		)

	parser.add_argument('--db', default="tpchbig.db", help="Name of the database file. Will be automatically created in the databases/ directory and overwrite any existing file")
	parser.add_argument('-s', '--scale', default=1, type=float, help="Scale factor for dbgen")
	parser.add_argument('-n', '--noise', default=0.1, type=float, help="Ratio of null values")
	parser.add_argument('-b', '--backend', default="sqlite", help="Backend (sqlite / oracle)")

	args = parser.parse_args()
	os.chdir(mimirhome)
	generatetables(args.scale)

	if args.backend.lower() == "sqlite":
		sqliteload(args.db, args.noise)
	elif args.backend.lower() == "oracle":
		oracleload(args.db, args.noise)
	else:
		print("Unrecognized backend {0}, exiting...".format(args.backend))
		sys.exit(0)


