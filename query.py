import pprint

# usse type_query() to find different type of data
def type_query(a):
    # using query to find numbers of node.
    query = {"type" : a}
    return query


# use get_db() to connnect to the MongoDb
def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

# use find() to run query in the package "tianjin" in the database
def find(db, query):
    return db.tianjin.find(query)


# use the aggregate() to run the pipeline in the package "tianjin" in the database
def aggregate(db, pipeline):
    return [doc for doc in db.tianjin.aggregate(pipeline)]



if __name__ == "__main__":
    # find the database named "udacity" and connect
    db = get_db('udacity')
    print "total number of doc"
    print db.tianjin.find().count()

    # find the size of nodes 
    node_query = type_query("node")
    node_results = find(db, node_query)

    print "total number of node"
    print node_results.count()
    print "Printing first 3 results\n"
    
    for nodes in node_results[:3]:
        pprint.pprint(nodes)

    # find the size of ways
    way_query = type_query("way")
    way_results = find(db, way_query)

    print "total number of way"
    print way_results.count()
    
    # find the query with information written in chinese
    tag_query = {"id":"3904051392"} 
    tag_results = find(db, tag_query)
    
    print tag_results.count()
    for tags in tag_results:
        pprint.pprint(tags)
    
    # build the pipeline to find the postcode
    post_pipeline = [{"$group":{"_id": "$address.postcode",
                                   "count":{"$sum":1 }}},
                        { "$sort":{"count" : -1}}]
    post_result = aggregate(db, post_pipeline)
    print post_result

    # build the pipeline to change the postcode
    re_post_pipeline = [{"$match" : {"address.postcode": {"$type": "int"},
                                     "address.postcode": {"$gt" : 300000}}},
                        {"$group":{"_id": "$address.postcode",
                                   "count":{"$sum":1 }}},
                        { "$sort":{"count" : -1}}]
    re_post_result = aggregate(db, re_post_pipeline)
    print re_post_result
    

    # build the pipeline to find the street information in address
    road_pipeline = [
                     {"$group":{"_id": "$address.street",
                                   "count":{"$sum":1 }}},
                        { "$sort":{"count" : -1}}]
    road_result = aggregate(db, road_pipeline)
    pprint.pprint(road_result)


    # build the pipeline to find the user donate the most
    top_user_pipeline = [{"$group":{"_id": "$created.user",
                           "count":{"$sum":1 }}},
                { "$sort":{"count" : -1}},
                {"$limit":1}]
    top_user_result = aggregate(db, top_user_pipeline)
    print top_user_result

    
    
    # find the number of distinct user 
    user_query = "created.user"
    print len(db.tianjin.distinct(user_query))

    # find those user just donate once
    user_once_pipeline = [{"$group":{"_id": "$created.user",
                                     "count":{"$sum":1 }}},
                          {"$group":{"_id": "$count",
                                     "num_users":{"$sum":1 }}},
                          { "$sort":{"_id" : 1}},
                          {"$limit":1}]
    user_once_result = aggregate(db, user_once_pipeline)
    print user_once_result   


    # use the pipeline to find the top 10 amenity type
    amenity_pipeline = [{"$match":{"amenity":{"$exists":1}}},
                        {"$group":{"_id":"$amenity","count":{"$sum":1}}},
                        {"$sort":{"count":-1}},{"$limit":10}]
    pprint.pprint(aggregate(db, amenity_pipeline))

    # use the pipeline to find the  most populat cuisine in restaurant
    cuisine_pipeline = [{"$match":{"amenity":{"$exists":1}, "amenity":"restaurant"}},
                         {"$group":{"_id":"$cuisine","count":{"$sum":1}}},
                         {"$sort":{"count":1}},{"$limit":2}]

    pprint.pprint(aggregate(db, cuisine_pipeline))

    
    
    
