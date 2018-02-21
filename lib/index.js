var util = require('util');

var _ = require('lodash');
var Busboy = require('busboy');
var GridFS = require('gridfs-stream');
var brake = require('brake');
const { PassThrough, Writable } = require('stream');

var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;

module.exports = GridFSService;

function GridFSService(options) {
  if (!(this instanceof GridFSService)) {
    return new GridFSService(options);
  }

  this.options = options;
  this.options.limit = this.options.limit || 0;
}

/**
 * Connect to mongodb if necessary.
 */
GridFSService.prototype.connect = function (cb) {
  var self = this;

  if (!this.db) {
    var url = (self.options.username && self.options.password) ?
      'mongodb://{$username}:{$password}@{$host}:{$port}/{$database}' :
      'mongodb://{$host}:{$port}/{$database}';

    // replace variables
    url = url.replace(/\{\$([a-zA-Z0-9]+)\}/g, function (pattern, option) {
      return self.options[option] || pattern;
    });

    // connect
    MongoClient.connect(url, function (error, db) {
      if (!error) {
        self.db = db;
      }
      return cb(error, db);
    });
  }
};

/**
 * List all storage containers.
 */
GridFSService.prototype.getContainers = function (cb) {
  var collection = this.db.collection('fs.files');

  collection.find({
    'metadata.container': { $exists: true }
  }).toArray(function (error, files) {
    var containerList = [];

    if (!error) {
      containerList = _(files)
        .map('metadata.container').uniq().value();
    }

    return cb(error, containerList);
  });
};


/**
 * Delete an existing storage container.
 */
GridFSService.prototype.deleteContainer = function (containerName, cb) {
  var collection = this.db.collection('fs.files');

  collection.deleteMany({
    'metadata.container': containerName
  }, function (error) {
    return cb(error);
  });
};


/**
 * List all files within the given container.
 */
GridFSService.prototype.getFiles = function (containerName, cb) {
  var collection = this.db.collection('fs.files');

  collection.find({
    'metadata.container': containerName
  }).toArray(function (error, container) {
    return cb(error, container);
  });
};


/**
 * Return a file with the given id within the given container.
 */
GridFSService.prototype.getFile = function (containerName, fileId, cb) {
  var collection = this.db.collection('fs.files');

  collection.find({
    '_id': new mongodb.ObjectID(fileId),
    'metadata.container': containerName
  }).limit(1).next(function (error, file) {
    if (!file) {
      error = new Error('getFile File not found');
      error.status = 404;
    }
    return cb(error, file || {});
  });
};


/**
 * Delete an existing file with the given id within the given container.
 */
GridFSService.prototype.deleteFile = function (containerName, fileId, cb) {
  var collection = this.db.collection('fs.files');

  collection.deleteOne({
    '_id': new mongodb.ObjectID(fileId),
    'metadata.container': containerName
  }, function (error) {
    return cb(error);
  });
};


/**
 * Upload middleware for the HTTP request.
 */
GridFSService.prototype.upload = function (containerName, req, cb) {
  var self = this;

  var busboy = new Busboy({
    headers: req.headers
  });

  busboy.on('file', function (fieldname, file, filename, encoding, mimetype) {
    if(mimetype == 'audio/mp3'){
      mimetype = 'audio/mpeg'
    };
    
    var options = {
      _id: new mongodb.ObjectID(),
      filename: filename,
      metadata: {
        container: containerName,
        filename: filename,
        mimetype: mimetype
      },
      mode: 'w'
    };

    var gridfs = new GridFS(self.db, mongodb);
    var stream = gridfs.createWriteStream(options);

    stream.on('close', function (file) {
      var collection = self.db.collection('fs.files');
      var identity = {
        'length': file.length,
        'md5': file.md5,
        'metadata.container': file.metadata.container
      };
      collection.find(identity).count((error, count) => {
        if (count === 0) {
          error = new Error('Creating file failed');
        }
        if (error) {
          return cb(error);
        }
        if (count === 1) {
          return cb(null, file);
        }
        collection.deleteOne({
          '_id': new mongodb.ObjectID(file._id),
          'metadata.container': file.metadata.container
        }, function (error) {
          if (error) {
            return cb(error);
          }
          collection.find(identity).limit(1).next(function (error, file) {
            if (!file) {
              error = new Error('File not found');
              error.status = 404;
            }
            file = file || {}
            file.duplicatedFile = true
            return cb(error, file);
          });
        });
      })
    });

    stream.on('error', cb);

    file.pipe(stream);
  });

  req.pipe(busboy);
};

/**
 * Get the stream for uploading
 * @param {String} container Container name
 * @param {String} file  File name
 * @param {Object} options Options for uploading
 * @returns {Stream} Stream for uploading
 */
GridFSService.prototype.uploadStream = function (container, file, options) {
  if (typeof options !== 'object' || options === null) {
    cb(new Error("options should be a non null object"));
    return
  }
  if (container) {
    options.container = container;
  }
  if (file) {
    options.remote = file;
  }

  let identity = {
    'length': options.length,
    'md5': options.md5,
    'metadata.container': options.container
  };

  let passThrough = new PassThrough()

  let collection = this.db.collection('fs.files');
  collection.find(identity).count((error, count) => {
    if (count >=1 ) {
      const writable = new Writable();
      writable._write = function (chunk, encoding, callback) {
        callback()
      }
      passThrough.pipe(writable)
      collection.find(identity).limit(1).next(function (error, file) {
        if (!file) {
          error = new Error('File not found');
          error.status = 404;
          passThrough.destroy(error)
          return
        }
        file.duplicatedFile = true
        passThrough.emit('close', file)
      })
    } else {
      let gridFsOptions = {
        _id: new mongodb.ObjectID(),
        filename: options.remote,
        metadata: {
          container: options.container,
          filename: options.remote,
          mimetype: options.mimetype
        },
        mode: 'w'
      }
      let gridfs = new GridFS(this.db, mongodb);
      let gridfsStream = gridfs.createWriteStream(gridFsOptions);
      passThrough.pipe(gridfsStream)
      gridfsStream.on('close', (file)=>{
        passThrough.emit('close', file)
      })
    }
  })
  return passThrough
}

/**
 * Download middleware for the HTTP request.
 */
GridFSService.prototype.download = function (containerName, fileId, req, res, cb) {
  this.getFile(containerName, fileId, (error, file)=> {
    if (!file) {
      error = new Error('download File not found');
      error.status = 404;
    }

    if (error) {
      return cb(error);
    }

    let metadata;
    if(file.metadata.mimetype == "audio/mp3") {
      metadata = "audio/mpeg"
    } else {
      metadata = file.metadata.mimetype;
    }

    let resHeaders = {
      'Content-Type':  metadata,
      'Content-Length': file.length,
      'Content-Range': 'bytes ' + 0 + '-',
      'Accept-Ranges': 'bytes'
    };

    var options = {
      startPos: 0,
      endPos: file.length - 1
    };

    var statusCode = 200;

    if(req.headers['range']) {
      var parts = req.headers['range'].replace(/bytes=/, "").split("-");
      var partialstart = parts[0];
      var partialend = parts[1];
      var start = parseInt(partialstart, 10);
      var end = partialend ? parseInt(partialend, 10) : file.length -1;
      var chunksize = (end-start)+1;
      
      //  Set headers
      resHeaders['Content-Range'] = 'bytes ' + start + '-' + end + '/' + file.length;
      resHeaders['Accept-Ranges'] = 'bytes';
      resHeaders['Content-Length'] = chunksize;
      //  Set Range
      options['startPos'] = start;
      options['endPos'] = end;
      //  Set status code
      statusCode = 206;
    } 

    res.writeHead(statusCode, resHeaders);

    try {
      const downStream = this.downloadStream(containerName, fileId, options)
      downStream.pipe(res)
    } catch (error) {
      cb(error)
    }
  })
};

/**
 * Get the stream for downloading.
 * @param {String} container Container name.
 * @param {String} fileId File id in the storage service.
 * @param {Object} options Options for downloading {startPos, endPos} means [startPos, endPos] inclusive
 * @returns {Stream} Stream for downloading
 */
GridFSService.prototype.downloadStream = function (container, fileId, options) {
  if (typeof options === 'function') {
    options = {};
  }
  options = options || {};
  if (container) {
    options.container = container;
  }
  if (fileId) {
    options.fileId = fileId;
  }

  let passThrough = new PassThrough()

  let gridRange = {
    startPos: options.startPos,
    endPos: options.endPos,
  }
  let gridfs = new GridFS(this.db, mongodb);
  let stream = gridfs.createReadStream({
    _id: new mongodb.ObjectID(options.fileId),
    range: gridRange
  })
  //  Set Bandwidth Limit if defined
  if (typeof this.options.limit === 'number' && this.options.limit > 0) {
    return stream.pipe(brake(this.options.limit)).pipe(passThrough);
  } else {
    return stream.pipe(passThrough);
  }
  return passThrough
}

GridFSService.modelName = 'storage';

/*
 * Routing options
 */

/*
 * GET /FileContainers
 */
GridFSService.prototype.getContainers.shared = true;
GridFSService.prototype.getContainers.accepts = [];
GridFSService.prototype.getContainers.returns = {
  arg: 'containers',
  type: 'array',
  root: true
};
GridFSService.prototype.getContainers.http = {
  verb: 'get',
  path: '/'
};

/*
 * DELETE /FileContainers/:containerName
 */
GridFSService.prototype.deleteContainer.shared = true;
GridFSService.prototype.deleteContainer.accepts = [
  { arg: 'containerName', type: 'string', description: 'Container name' }
];
GridFSService.prototype.deleteContainer.returns = {};
GridFSService.prototype.deleteContainer.http = {
  verb: 'delete',
  path: '/:containerName'
};

/*
 * GET /FileContainers/:containerName/files
 */
GridFSService.prototype.getFiles.shared = true;
GridFSService.prototype.getFiles.accepts = [
  { arg: 'containerName', type: 'string', description: 'Container name' }
];
GridFSService.prototype.getFiles.returns = {
  type: 'array',
  root: true
};
GridFSService.prototype.getFiles.http = {
  verb: 'get',
  path: '/:containerName/files'
};

/*
 * GET /FileContainers/:containerName/files/:fileId
 */
GridFSService.prototype.getFile.shared = true;
GridFSService.prototype.getFile.accepts = [
  { arg: 'containerName', type: 'string', description: 'Container name' },
  { arg: 'fileId', type: 'string', description: 'File id' }
];
GridFSService.prototype.getFile.returns = {
  type: 'object',
  root: true
};
GridFSService.prototype.getFile.http = {
  verb: 'get',
  path: '/:containerName/files/:fileId'
};

/*
 * DELETE /FileContainers/:containerName/files/:fileId
 */
GridFSService.prototype.deleteFile.shared = true;
GridFSService.prototype.deleteFile.accepts = [
  { arg: 'containerName', type: 'string', description: 'Container name' },
  { arg: 'fileId', type: 'string', description: 'File id' }
];
GridFSService.prototype.deleteFile.returns = {};
GridFSService.prototype.deleteFile.http = {
  verb: 'delete',
  path: '/:containerName/files/:fileId'
};

/*
 * DELETE /FileContainers/:containerName/upload
 */
GridFSService.prototype.upload.shared = true;
GridFSService.prototype.upload.accepts = [
  { arg: 'containerName', type: 'string', description: 'Container name' },
  { arg: 'req', type: 'object', http: { source: 'req' } }
];
GridFSService.prototype.upload.returns = {
  arg: 'file',
  type: 'object',
  root: true
};
GridFSService.prototype.upload.http = {
  verb: 'post',
  path: '/:containerName/upload'
};

/*
 * GET /FileContainers/:containerName/download/:fileId
 */
GridFSService.prototype.download.shared = true;
GridFSService.prototype.download.accepts = [
  { arg: 'containerName', type: 'string', description: 'Container name' },
  { arg: 'fileId', type: 'string', description: 'File id' },
  { arg: 'req', type: 'object', 'http': { source: 'req' } },
  { arg: 'res', type: 'object', 'http': { source: 'res' } }
];
GridFSService.prototype.download.http = {
  verb: 'get',
  path: '/:containerName/download/:fileId'
};
