#!/usr/bin/python

import sqlite3
import os
import random
import sys
import argparse

mimirhome = os.path.dirname(os.path.dirname(os.getcwd()))

lensesinit = """
		CREATE TABLE MIMIR_LENSES(
			name varchar(30), 
			query text, 
			lens_type varchar(30),
			parameters text,
			PRIMARY KEY(name)
		)
	"""

rSchema = """
		CREATE TABLE R (
			A int,
			B int,
			C int
		)
	"""

sSchema = """
		CREATE TABLE S (
			B int,
			C int,
			D int
		)
	"""

tSchema = """
		CREATE TABLE T (
			C int,
			D int,
			E int
		)
	"""

seq = [x for x in range(10)]



def createDB(db, size, noise, seed):
	database = os.path.join(mimirhome, "databases", db)

	# Delete database if it already exists
	try:
		os.remove(database)
	except OSError:
		pass

	# Create database
	conn = sqlite3.connect(database)
	c = conn.cursor()
	
	# Create tables
	c.execute(lensesinit)
	c.execute(rSchema)
	c.execute(sSchema)
	c.execute(tSchema)

	# Populate tables
	random.seed(seed)

	for i in ['R', 'S', 'T']:
		tuples = [(randomElem(noise), randomElem(noise), randomElem(noise)) for x in range(size)]
		c.executemany('INSERT INTO '+i+' VALUES (?, ?, ?)', tuples)

	conn.commit()
	conn.close()



def randomElem(noise):
	return random.choice(seq) if random.random() > noise else None	



def main():
	parser = argparse.ArgumentParser(description= """
		Generates a sqlite database with noisy tables. Three tables are produced -
			R(A int, B int, C int),
			S(B int, C int, D int),
			T(C int, D int, E int)
		"""
		)

	parser.add_argument('--db', default="noisy.db", help="Name of the database file. Will be automatically created in the databases/ directory and overwrite any existing file")
	parser.add_argument('-S', '--size', default=1000, type=int, help="Number of tuples for each table")
	parser.add_argument('-n', '--noise', default=0.2, type=float, help="Ratio of null values")
	parser.add_argument('-s', '--seed', default=7, type=int, help="Seed value for random number generator")

	args = parser.parse_args()
	
	createDB(args.db, args.size, args.noise, args.seed)
	


if(__name__ == "__main__"):
	main()