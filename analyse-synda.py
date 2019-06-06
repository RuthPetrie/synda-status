#!/usr/bin/env Python

import sqlite3
import os
import matplotlib.pyplot as plt
import datetime
from sqlite3 import Error
plt.switch_backend('agg')
database = "/gws/nopw/j04/cmip6_prep_vol1/synda/cmip6_sdt_backups/sdt.db.latest"


def create_connection(db_file):
	""" create a database connection to the SQLite database
		specified by the db_file
	:param db_file: database file
	:return: Connection object or None
	"""
	try:
		conn = sqlite3.connect(db_file)
		return conn
	except Error as e:
		print(e)

	return None


def get_data_transfer_vols(conn):
	"""
	Retrieve the volume of data transferred per datanode

	:param conn: Database connection
	:return: datanodes [list], volumes [list]
	"""

	query = conn.cursor()
	query.execute(
		"select data_node, cast(sum(size)/(1024*1024*1024*1024.) as float) from file where status='done' group by data_node;")
	results = query.fetchall()

	nodes = []
	vols = []
	for i in results:
		nodes.append(i[0])
		vols.append(i[1])

	return nodes, vols


def get_data_transfer_rates(conn, datanodes):
	"""
	Retrieve the data transfer rates per datanode

	:param conn: Database connection
	:return: rates[[],[]]
	"""

	result = []
	for datanode in datanodes:
		query = conn.cursor()
		query.execute("select rate/1000000 from file where status='done' and data_node=?", (datanode,))
		results = query.fetchall()
		result.append([r[0] for r in results])

	return result


def plot_data_vols(nodes, volumes, title):
	plt.figure(0)
	plt.title(title)
	plt.bar(nodes, volumes)
	plt.ylabel('Volume Transferred (TB)')
	plt.xticks(rotation=90)
	plt.tight_layout()
	plt.savefig('/home/users/rpetrie/synda/synda-status/docs/images/volumes.png', format='png')


def plot_transfer_rates(datanodes, transfer_rates, title):
	plt.figure(1)
	plt.title(title)
	plt.boxplot(transfer_rates, labels=datanodes)
	plt.ylabel('Transfer rate (MiB/s)')
	plt.xticks(rotation=90)
	plt.tight_layout()
	plt.savefig('/home/users/rpetrie/synda/synda-status/docs/images/rates.png', format='png')


def main():

	title = datetime.datetime.now().strftime("%Y-%m-%d")
	conn = create_connection(database)
	datanodes, volumes = get_data_transfer_vols(conn)
	plot_data_vols(datanodes, volumes, title)
	transfer_rates = get_data_transfer_rates(conn, datanodes)
	plot_transfer_rates(datanodes, transfer_rates, title)


if __name__ == "__main__":
	main()

