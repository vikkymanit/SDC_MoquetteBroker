import json
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import tostring

def dict_to_xml(tag, d):
	'''
	Turn a simple dict of key/value pairs into XML
	'''
	elem = Element(tag)
	for key, val in d.items():
		child = Element(key)
		child.text = str(val)
		elem.append(child)
	return elem

debsfile = '/home/chris/datasets/sdcdata/debs2015/sorted_data.csv'
jsonfile = '/home/chris/datasets/sdcdata/debs2015/debs2015.json'
xmlfile = '/home/chris/datasets/sdcdata/debs2015/debs2015.xml'

with open(debsfile, 'r') as csv:
	with open(jsonfile, 'w') as jsonres:
		with open(xmlfile, 'w') as xmlres:
			for totalline in csv:
				line = totalline.strip().split(',')
				if len(line) != 17:
					print("invalid line")
					continue
				dc = dict()

				dc['medallion'] = line[0]
				dc['hack_license'] = line[1]
				dc['pickup_datetime'] = line[2]
				dc['dropoff_datetime'] = line[3]
				dc['trip_time_in_secs'] = line[4]
				dc['trip_distance'] = line[5]
				dc['pickup_longitude'] = line[6]
				dc['pickup_latitude'] = line[7]
				dc['dropoff_longitude'] = line[8]
				dc['dropoff_latitude'] = line[9]
				dc['payment_type'] = line[10]
				dc['fare_amount'] = line[11]
				dc['surcharge'] = line[12]
				dc['mta_tax'] = line[13]
				dc['tip_amount'] = line[14]
				dc['tolls_amount'] = line[15]
				dc['total_amount'] = line[16]

				json.dump(dc, jsonres)
				jsonres.write('\n')


				xmlres.write(tostring(dict_to_xml('trip', dc)).decode('ascii'))
				xmlres.write('\n')

print('finished writing')


