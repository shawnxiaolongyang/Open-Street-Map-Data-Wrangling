#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json
"""
Your task is to wrangle the data and transform the shape of the data
into the model we mentioned earlier. The output should be a list of dictionaries
that look like this:


You have to complete the function 'shape_element'.
We have provided a function that will parse the map file, and call the function with the element
as an argument. You should return a dictionary, containing the shaped data for that element.
We have also provided a way to save the data in a file, so that you could use
mongoimport later on to import the shaped data into MongoDB. 

Note that in this exercise we do not use the 'update street name' procedures
you worked on in the previous exercise. If you are using this code in your final
project, you are strongly encouraged to use the code from previous exercise to 
update the street names before you save them to JSON. 

In particular the following things should be done:
- you should process only 2 types of top level tags: "node" and "way"
- all attributes of "node" and "way" should be turned into regular key/value pairs, except:
    - attributes in the CREATED array should be added under a key "created"
    - attributes for latitude and longitude should be added to a "pos" array,
      for use in geospacial indexing. Make sure the values inside "pos" array are floats
      and not strings. 
- if second level tag "k" value contains problematic characters, it should be ignored
- if second level tag "k" value starts with "addr:", it should be added to a dictionary "address"
- if second level tag "k" value does not start with "addr:", but contains ":", you can process it
  same as any other tag.
- if there is a second ":" that separates the type/direction of a street,
  the tag should be ignored, for example:

"""

# use Regex code to distinguish the <tag> with attribute, but no ":" in the tag.
lower = re.compile(r'^([a-z]|_)*$')
# use Regex code to distinguish the <tag> with 1 lever attribute, namely all other attribute except "address" .
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
# use Regex code to replace problem characters
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
other = re.compile('[^a-z, A-Z,0-9,_]')



















#build a created vector to store the second level attribute in created.
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]



def key_type(element, keys):
    if element.tag == "tag":
        # YOUR CODE HERE
        k = element.attrib['k']
        if re.search(lower,k):
            keys['lower']+=1
        elif re.search(lower_colon,k):
            keys['lower_colon']+=1
        elif re.search(problemchars,k):
            keys['problemchars']+=1
        elif re.search(other,k):
            keys['other']+=1
            
        pass
        
    return keys


def shape_element(element):
    node = {}
    #since the main format of the openstreetmap data is constructed by node and way, we can restore the data on that. 
    if element.tag == "node" or element.tag == "way" :
        # give all the attribute their value
        node["id"] = element.attrib["id"]
        node["type"] =  element.tag      
        node[ "visible"] = element.get("visible")
        created = {}
        # since the element's name in "created" tag is same with "CREATED", we can use a for loop to add the value into the attribute
        for item in CREATED:
            created[item] = element.attrib[item]
        node["created"] = created
        # put the position element in the format of [latitude, longtitude]
        if "lat" in element.keys() and "lon" in element.keys():
           pos = [float(element.attrib["lat"]), float(element.attrib["lon"])]        
           node["pos"] = pos
        else:
           node["pos"] = None
        addr = {}
        # since the address and other information store in the tag, we need to use a "for" loop to read that out
        for tag in element.iter("tag"):
            tag_name = tag.attrib["k"]
            value = tag.attrib["v"]
            # use the Regex code to find out the attribute written in the <tag> format
            if re.search(lower,tag_name) or re.search(lower_colon,tag_name):
                if tag_name == "amenity":
                    node["amenity"] = value
                elif  tag_name == "cuisine": 
                     node["cuisine"] = value 
                elif  tag_name == "name": 
                     node["name"] = value
                elif  tag_name == "phone": 
                     node["phone"] = value  
            if "addr:" in tag_name:
                # the "addr:" tag means address, contains attribute such as "city", "housekeeper". In order to avoid "addr:city:housekeeper" occurs, we only use tag with one ":".
                if tag_name.split(":")[-1]==tag_name.split(":")[1]:
                    add_key = tag_name.split(":")[1]
                    if add_key == "postcode":
                        if value != None:
                            addr[add_key] = int(value)
                    else:
                        addr[add_key] = value
                node["address"] = addr  
        # since one "way" is consisted of huge amount of "nodes", we use the "ref" to save the relationship
        if element.tag == "way":
            nd_refs = []
            for nd in element.iter("nd"):
                if "ref" in nd.keys():
                   nd_refs.append(nd.get("ref"))
            node["node_refs"] = nd_refs
        
        
        return node
    else:
        return None
    
#def element_cleam(node):
    # clean the problem char
 #   for i in node:
  #      if re.search(problemchars,i):
   #         lower.search(i)
            


    # return node

        

def process_map(file_in, pretty = False):
    # read out the data of osm, transfe into the json file
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            # use shape_element set before to put the data from osm into the json format
            keys = key_type(element, keys)
            el = shape_element(element)
            #el = element_clean(el)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    pprint.pprint(keys)
    print data[-1]
    return data

def test():
    #  use the process_map function to transfer the data from osm to json 
    data = process_map('tianjin_china.osm', True)
    pprint.pprint(data[0:10])
    
    
    
if __name__ == "__main__":
    test()
