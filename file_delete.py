import time
from datetime import datetime
import json
import qumulo
from qumulo.rest_client import RestClient
import logging

# Logging Details
logging.basicConfig(handlers=[logging.FileHandler(filename='file_delete_activity.log', encoding='utf-8')],
                    level=logging.INFO,
                    format='%(asctime)s,%(levelname)s,%(message)s')

# Read credentials
json_file = open('credentials.json','r')
json_data = json_file.read()
json_object = json.loads(json_data)

# Parse cluster credentials
cluster_address = json_object['cluster_address']
port_number = json_object['port_number']
username = json_object['username']
password = json_object['password']

# Connect to the cluster
rc = RestClient(cluster_address, port_number)
rc.login(username, password)
logging.info('Connection established with {}'.format(cluster_address))

# Script inputs
directory_path = json_object['directory_path']
last_date = json_object['days']



def file_operation (object):
	modification_time = object['modification_time'].split("T")[0]
	modification_time_epoch = datetime.strptime(modification_time, '%Y-%m-%d').strftime("%s")
	current_time = datetime.now().strftime("%s")
	diff_time = int(current_time) - int(modification_time_epoch)
	if diff_time >= (86400 * int(last_date)):
		rc.fs.delete(object['path'])
		logging.info('{} file was deleted. Modification time is {}'.format(object['path'],modification_time))
	else:
		print ("Processing...", end='\r')


def tree_walk (objects):
	for r in range(len(objects)):
		object = objects[r]
		if object['type'] == "FS_FILE_TYPE_FILE":
			file_operation (object)
		elif object['type'] == "FS_FILE_TYPE_DIRECTORY":
			next_page = "first"
			while next_page != "":
				r = None
				if next_page == "first":
					try:
						r = rc.fs.read_directory(path=object['path'], page_size=1000)
					except:
						next
				else:
					r = rc.request("GET", next_page)
				if not r:
					break
				dir_objects = r['files']
				tree_walk (dir_objects)
				if 'paging' in r and 'next' in r['paging']:
					next_page = r['paging']['next']
				else:
					next_page = ""

# Take a snapshot of the directory
rc.snapshot.create_snapshot('file_delete_activity','', '15days', directory_path)
snapshots = rc.snapshot.list_snapshots()
snapshot_id_list = []
sorted_snapshot_id_list = []

for individual_snapshot in snapshots['entries']:
	if 'file_delete_activity' == individual_snapshot['name']:
		snapshot_id_list.append(individual_snapshot['id'])
		
sorted_snapshot_id_list = sorted(snapshot_id_list)
newer_snapshot_id = sorted_snapshot_id_list[-1]
logging.info('A new snapshot was created. snapshot ID is {} for {}'.format(newer_snapshot_id, directory_path))

# Inherit file scan operations
objects = rc.fs.read_directory(path=directory_path, page_size=10000)['files']
tree_walk (objects)
