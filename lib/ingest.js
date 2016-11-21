#!/usr/bin/node

'use strict';

var elasticsearch = require('elasticsearch'),
    mongoose = require('mongoose');

var argv = require('minimist')(process.argv.slice(2));

var async = require('async');

if (argv.help){
    console.log(`
    Script to import user data into Elastic Search.
    This version imports each document in series. To import in parallel (faster)
    see ingest-parallel.js.

    Usage:
      --dev   Use development config settings, show extra info.
      --dev2  Use juju machine 13.

    `);
    process.exit();
}

var currWorkingDir = process.cwd();

// this will get updated by the user count, then checked at end to see if it matches the
// the number of indices actually created.
var gIndexTotal = 0;
// running total of number of users for which ingesting is finished
var gIngestedTotal = 0;
// keep track of users with no documents to ingest
var gEmptyUsers = [];

var flags = {};

// URIs
var metaDbUri = "juju-machine-13-lxc-2.prod.cittacloud.net:27017",
    mongodbUri = "juju-machine-10-lxc-2.prod.cittacloud.net:27017",
    elasticSearchUri = "juju-machine-13-lxc-3.prod.cittacloud.net:9200";

if (argv.dev){
    mongodbUri = "10.0.3.131:27017";
    metaDbUri = "10.0.3.131:27017";
    elasticSearchUri = '10.0.3.221:9200';
}
if (argv.dev2){
    mongodbUri = "juju-machine-3-lxc-1.dev.cittacloud.net:27017";
    metaDbUri = "juju-machine-3-lxc-2.dev.cittacloud.net:27017";
    elasticSearchUri = 'juju-machine-2-lxc-6.dev.cittacloud.net:9200';
}
if (argv.dev3) {
    mongodbUri = "localhost:27017";
    metaDbUri = "localhost:27017";
    elasticSearchUri = '10.0.3.177:9200';
}

if (argv.dev2 || argv.dev3){
    argv.dev = true; // turn on extra reporting
}
var metaverseDbUri = "mongodb://streamsys:SuperDuper!@" + metaDbUri + "/StreamDb001",
    plexDbUri = "mongodb://" + mongodbUri;

var targetCollections = ['activity', 'document', 'contact', 'collection', 'contentitem', 'message', 'resource', 'task'];

// Set flush boolean
var deleteIndexes = true;

function ingestDocumentOfCollectionF(userid, document, collectionName, elasticClient, userDb, docTotal) {
    return  (callback) =>
        ingestDocumentOfCollection(userid, document, collectionName, elasticClient, userDb, docTotal, callback);
}

var ingestDocumentOfCollection = function(userid, document, collectionName, elasticClient, userDb, docTotal, cb) {

    var docId = document._id.toString();
    var pId = document.s.pId.toString();
    var pType = document.s.pType;
    var indexRec = {
        index: userid,
        type: collectionName,
        id: docId
    };

    // src is container object, dest is contained
    var _getLinkedItems = function(indexRec, pId) {

        // find all the tuples for which the current item is the srcObject (the container)
        userDb.models.tuple.find({'data.srcObj': docId}).exec(function (err, docs) {

            if (err) {
                console.warn(err);
                process.exit(1);
            }
            var doc;
            var linkedObjects = {};
            if (docs && (docs.length > 0)) {
                for (var i = 0; i < docs.length; i++) {
                    doc = docs[i];
                    var destCN = doc.data.destObjCN.toString(); // what kind of contained object?
                    // TODO: could add a check here to verify that destCN is in targetCollections
                    if (! linkedObjects[destCN]) {linkedObjects[destCN] = [];}
                    linkedObjects[destCN].push(doc.data.destObj.toString());
                }
            }

            _ingestDocument(indexRec, linkedObjects, pId);
        });
    };

    // this will ingest the document including arrays of the child items
    var _ingestDocument = function(indexRec, linkedObjects, pId) {

        //var cards = [];
        switch (collectionName) {
            case 'contentitem':
                indexRec.body = {
                    body: document.data.body,
                    topic: document.data.topic
                };
                break;
            default:
                indexRec.body = {
                    topic: document.data.title
                };
        }
        for(var k in linkedObjects){
           indexRec.body[k] = linkedObjects[k];
        }

        // got the list of linked objects, now put object into ES
        elasticClient.index(indexRec, function (err, resp) {
            if (err) {
                console.error(err.message);
            } else {

                    if (argv.dev) {
                        console.log('Finished ingesting ', resp._type, resp._id);
                    }
                    _updateFlags();
                    return cb();
            }
        });
    };

    var _updateFlags = function(){

        // the flags object is used to track when we're done
        flags[userid][collectionName][docId] = 1;

        // check to see if the current collection is finished
        if (Object.keys(flags[userid][collectionName]).length === docTotal){

            // the current collection of docs is finished
            flags[userid][collectionName] = true;
            console.log('Finished', collectionName, 'for user', userid);
            // now check if the other collections are also finished
            checkFinished(userid);

        }

    };

    // update the parent item
    // TODO: need to fix update flags if we use this
/*
    var _updateParent = function(pId) {

        var indexRec = {
            index: userid,
            type: pType,
            id: pId
        };
        indexRec.body = {};
        indexRec.body.doc = {};
        indexRec.body.upsert = {};
        indexRec.version = 1;
        indexRec.versionType = 'force'; // in ES 2.1 this changes to version_type
        indexRec.body.doc[collectionName] = [docId];
        indexRec.body.upsert[collectionName] = [docId];

        // got the list of linked objects, now put object into ES
        elasticClient.update(indexRec, function (err, resp) {
            if (err) {
                console.error(err.message);
            } else {
                return;

            }
        });
    };
*/

    // if the pType of a contentitem is 'contentitem' then it
    // is a previous version of that contentitem, so we don't
    // need to index it.
    if (document.s.pType === 'contentitem') {
        if (argv.dev) {
            console.log('Ignoring content item version');
        }
        _updateFlags();
        return cb();
    }

    process.nextTick( ()=> _getLinkedItems(indexRec, pId) );

};

// process one collection for one user
var processUserCollectionF = function (userid, userDb, collectionName, elasticClient) {
   return callback => processUserCollection (userid, userDb, collectionName, elasticClient, callback)
}

var processUserCollection = function (userid, userDb, collectionName, elasticClient, cb) {
    userDb.models[collectionName].find(function (err, collDocs) {
        if (err) {
            console.log('Failed to open ' + collectionName + ' error: ' + err.message);
        } else {
            // Iterate through all the docs in the collection
            var docTotal = collDocs.length;
            var funcs = [];
            if (collDocs.length) {
                flags[userid][collectionName] = {};
            } else {
                flags[userid][collectionName] = 'blank'; // done processing this collection for this user
                checkFinished(userid);
            }
            for (var i = 0; i < collDocs.length; i++){
                funcs.push (ingestDocumentOfCollectionF(userid, collDocs[i], collectionName, elasticClient, userDb, docTotal));
            }
            async.series(funcs, (err, results) => {
                console.log('Ended processing collection: ' + collectionName);
                cb();
            })
        }
    });

};
/**
 * Keep count of users with no documents.
 * @param userid
 */
var checkFinished = function(userid){

    // check if we've processed all of the collections of this user.
    // If so, we'll have a key for each collection and it will be ===
    // true, not an object.
    if (Object.keys(flags[userid]).length !== targetCollections.length){
        return;
    }

    // check if all of the collections are 'blank' - means
    // all were skipped i.e. no documents for that user were ingested
    var empty_user = true;
    for (var k in flags[userid]){
        if (flags[userid][k] !== 'blank') {  // if ia collection is not blank,
            empty_user = false;
        }
        if (flags[userid][k] === 'blank'){
            flags[userid][k] = true; // 'blank' also means we're done processing it
        }
    }
    if (empty_user){
        gEmptyUsers.push(userid);
    }
};



// print number of mappings and indices, do a couple of sanity checks
var printReport = function(){

    var elasticClient = client;

    console.log("\nElastic Search has finished ingesting user data.");

    var statReport = function() {
        elasticClient.indices.stats(function
                (err, resp) {
                if (err) {
                    console.error('error on report stats', err.message);
                } else {
                    console.log('Indexed doc count: ', resp._all.primaries.indexing.index_total);
                    console.log('Indexed doc size in bytes: ', resp._all.primaries.store.size_in_bytes.toString());
                    var indices = resp.indices;
                    var indexCount = Object.keys(indices).length;
                    console.log('Number of indices: ', indexCount);

                    console.assert(indexCount === (gIngestedTotal - gEmptyUsers.length), "Bad index count! " + gIngestedTotal);
                    if (gEmptyUsers.length) {
                        console.log("These users had no documents:", gEmptyUsers);
                    }
                    process.exit(0);
                }
            }
        );
    };
    var mappingReport = function(callback) {
        elasticClient.indices.getMapping(function
                (err, resp) {
                if (err) {
                    console.error('error on report mapping', err.message);
                } else {
                    var firstIndex = resp[Object.keys(resp)[0]];
                    var mappingCount = Object.keys(firstIndex.mappings).length;
                    console.log("Mappings: ", mappingCount);
                    console.assert(mappingCount = targetCollections.length);
                    callback();
                }
            }
        );
    };

    mappingReport(statReport);

};


var startIngest = function (metaConn, plexConn, elasticClient) {
    // Get User listing
    metaConn.models.User.find(function (err, userDocs) {
        if (err) {
            console.log('Failed to get users from Metaverse: ' + err.message);
        } else {
            console.log('Processing num of users: ', userDocs.length);

            if (userDocs.length > 0) {

                // Get User DB per user, update user count
                // Loop through users
                gIndexTotal = userDocs.length;
                var funcs = [];
                for (var i = 0, maxI = gIndexTotal; i < maxI; i++) {
                    console.log('Processing user');
                    funcs.push(_ingestUserF(userDocs[i]));
                }
                async.series(funcs, (err, results) => {
                    console.log('Ended processing for all users');
                    printReport();
               });
            }
        }
    });

    function _ingestUserF(userDoc){
        return callback => _ingestUser(userDoc, callback);
    }
    function _ingestUser(userDoc, cb) {
        var funcs = []; // will make a function for each user and collection
        var username = userDoc.username;
        var userid = userDoc._id.toString();
        console.log(`Starting ingest for ${username} ${userid}`);
        var userDbName = userid + '_Primary',
            userDb = plexConn.useDb(userDbName);

        require(currWorkingDir + '/import/solaria/models-v1/Activity')(userDb, mongoose);
        require(currWorkingDir + '/import/solaria/models-v1/Contact')(userDb, mongoose);
        require(currWorkingDir + '/import/solaria/models-v1/Collection')(userDb, mongoose);
        require(currWorkingDir + '/import/solaria/models-v1/Resource')(userDb, mongoose);
        require(currWorkingDir + '/import/solaria/models-v1/ContentItem')(userDb, mongoose);
        require(currWorkingDir + '/import/solaria/models-v1/Document')(userDb, mongoose);
        require(currWorkingDir + '/import/solaria/models-v1/Message')(userDb, mongoose);
        require(currWorkingDir + '/import/solaria/models-v1/Task')(userDb, mongoose);
        require(currWorkingDir + '/import/solaria/models-v1/Tuple')(userDb, mongoose);

        var collectionCount = 0;
        // ingest the docs for one user by collection
        flags[userid] = {};
        targetCollections.forEach(function (collectionName) {
            funcs.push(
                processUserCollectionF(userid, userDb, collectionName, elasticClient, collectionCount)
            )
        });
        // we got the a func for each collection, now ingest them in series
        async.series(funcs, (err, results) => {
            console.log(`Ended processing for ${username} ${userid}`);
            gIngestedTotal = gIngestedTotal + 1;
            cb();
        });
    }

};

// <editor-fold desc="startup">
var metaverseConnection = null;

// Connect the client to two nodes, requests will be
// load-balanced between them using round-robin
var client = elasticsearch.Client({
    hosts: [elasticSearchUri]
});


var connectToDatabases = function () {
    // Connect with Mongoose
    metaverseConnection = mongoose.createConnection(metaverseDbUri);
    metaverseConnection.on('error', console.error.bind(console, 'Stream Directory Database connection error: '));
    metaverseConnection.once('open', function () {
        console.log('Connected to Directory Database');

        // Mongoose requires that the models be registered with the connection
        // i.e. it is not automatic that the models are added to the dbConn object.
        require(currWorkingDir + '/import/solaria/models-v1/User')(metaverseConnection, mongoose);

        var plexDbConn = mongoose.createConnection(plexDbUri);

        plexDbConn.on('error', console.error.bind(console, 'Stream Plex Database connection error: '));
        plexDbConn.once('open', function () {
            startIngest(metaverseConnection, plexDbConn, client);
        });
    });
};

// get the current status of the entire cluster.
// Note: params are always optional, you can just send a callback
client.cluster.health(function (err, resp) {
    if (err) {
        console.error(err.message);
    } else {

        // Delete indexes if flag is set to true
        if (deleteIndexes) {
            client.indices.delete({
                index: '_all'
            }, function (err, res) {

                if (err) {
                    console.error(err.message);
                } else {
                    console.log('Indexes have been deleted!');
                    connectToDatabases();
                }
            });
        } else {
            connectToDatabases();
        }

    }
});
