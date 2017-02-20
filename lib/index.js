import _ from 'lodash';
import async from 'async';
import Busboy from 'busboy';
import { DataSource } from 'loopback-datasource-juggler';
let debug       = require('debug')('loopback:storage:mongo');
import Grid from 'gridfs-stream';
import mongodb from 'mongodb';
import Promise from 'bluebird';

let { GridFS }      = mongodb;
let { ObjectID }    = mongodb;

let generateUrl = function(options) {
  let host      = options.host || options.hostname || 'localhost';
  let port      = options.port || 27017;
  let database  = options.database || 'test';
  if (options.username && options.password) {
    return `mongodb://${options.username}:${options.password}@${host}:${port}/${database}`;
  } else {
    return `mongodb://${host}:${port}/${database}`;
  }
};

class MongoStorage {
  constructor(settings) {
    this.settings = settings;
    if (!this.settings.url) {
      this.settings.url = generateUrl(this.settings);
    }
  }

  connect(callback) {
    let self = this;
    if (this.db) {
      return process.nextTick(function() {
        if (callback) {
          return callback(null, self.db);
        }
      });
    } else {
      return mongodb.MongoClient.connect(this.settings.url, this.settings, function(err, db) {
        if (!err) {
          debug(`Mongo connection established: ${self.settings.url}`);
          self.db = db;
        }
        if (callback) {
          return callback(err, db);
        }
      });
    }
  }

  getContainers(callback) {
    return this.db.collection('fs.files')
    .find({
      'metadata.mongo-storage': true})
    .toArray(function(err, files) {
      if (err) { return callback(err); }
      let list = _(files)
      .map('metadata')
      .flatten()
      .map('container')
      .uniq()
      .map(item => ({container: item})).value();
      return callback(null, list);
    });
  }

  getContainer(name, callback) {
    return this.db.collection('fs.files')
    .find({
      'metadata.mongo-storage': true,
      'metadata.container': name}).toArray(function(err, files) {
      if (err) { return callback(err); }
      return callback(null, {
        container: name,
        files
      }
      );
    });
  }

  destroyContainer(name, callback) {
    let self = this;
    return self.getFiles(name, function(err, files) {
      if (err) { return callback(err); }
      return async.each(files, (file, done) => self.removeFileById(file._id, done)
      , callback);
    });
  }

  upload(container, req, res, callback) {
    let self = this;
    let busboy = new Busboy({headers: req.headers});
    let promises = [];
    busboy.on('file', (fieldname, file, filename, encoding, mimetype) =>
      promises.push(new Promise(function(resolve, reject) {
        let options = {
          filename,
          metadata: {
            'mongo-storage': true,
            container,
            filename,
            mimetype
          }
        };
        return self.uploadFile(container, file, options, function(err, res) {
          if (err) { return reject(err); }
          return resolve(res);
        });
      })
      )
    );
    busboy.on('finish', () =>
      Promise.all(promises)
      .then(res => callback(null, res)).catch(callback)
    );
    return req.pipe(busboy);
  }

  uploadFile(container, file, options, callback) {
    if (callback == null) { callback = function() {  }; }
    options._id = new ObjectID();
    options.mode = 'w';
    let gfs = Grid(this.db, mongodb);
    let stream = gfs.createWriteStream(options);
    stream.on('close', metaData => callback(null, metaData));
    stream.on('error', callback);
    return file.pipe(stream);
  }

  getFiles(container, callback) {
    return this.db.collection('fs.files')
    .find({
      'metadata.mongo-storage': true,
      'metadata.container': container}).toArray(callback);
  }
  
  removeFile(container, filename, callback) {
    let self = this;
    return self.getFile(container, filename, function(err, file) {
      if (err) { return callback(err); }
      return self.removeFileById(file._id, callback);
    });
  }

  removeFileById(id, callback) {
    let self = this;
    return async.parallel([
      done =>
        self.db.collection('fs.chunks')
        .remove(
          {files_id: id}
        , done)
      ,
      done =>
        self.db.collection('fs.files')
        .remove(
          {_id: id}
        , done)
      
    ], callback);
  }

  __getFile(query, callback) {
    return this.db.collection('fs.files')
    .findOne(query
    , function(err, file) {
      if (err) { return callback(err); }
      if (!file) {
        err = new Error('File not found');
        err.status = 404;
        return callback(err);
      }
      return callback(null, file);
    });
  }

  getFile(container, filename, callback) {
    return this.__getFile({
      'metadata.mongo-storage': true,
      'metadata.container': container,
      'metadata.filename': filename
    }
    , callback);
  }

  getFileById(id, callback) {
    return this.__getFile({_id: id}, callback);
  }

  __download(file, res, callback) {
    if (callback == null) { callback = function() {  }; }
    let gfs = Grid(this.db, mongodb);
    let read = gfs.createReadStream({
      _id: file._id});
    res.set('Content-Disposition', `attachment; filename=\"${file.filename}\"`);
    res.set('Content-Type', file.metadata.mimetype);
    res.set('Content-Length', file.length);
    return read.pipe(res);
  }

  downloadById(id, res, callback) {
    if (callback == null) { callback = function() {  }; }
    let self = this;
    return this.getFileById(id, function(err, file) {
      if (err) { return callback(err); }
      return self.__download(file, res, callback);
    });
  }

  download(container, filename, res, callback) {
    if (callback == null) { callback = function() {  }; }
    let self = this;
    return this.getFile(container, filename, function(err, file) {
      if (err) { return callback(err); }
      return self.__download(file, res, callback);
    });
  }
}

MongoStorage.modelName = 'storage';

MongoStorage.prototype.getContainers.shared = true;
MongoStorage.prototype.getContainers.accepts = [];
MongoStorage.prototype.getContainers.returns = {arg: 'containers', type: 'array', root: true};
MongoStorage.prototype.getContainers.http = {verb: 'get', path: '/'};

MongoStorage.prototype.getContainer.shared = true;
MongoStorage.prototype.getContainer.accepts = [{arg: 'container', type: 'string'}];
MongoStorage.prototype.getContainer.returns = {arg: 'containers', type: 'object', root: true};
MongoStorage.prototype.getContainer.http = {verb: 'get', path: '/:container'};

MongoStorage.prototype.destroyContainer.shared = true;
MongoStorage.prototype.destroyContainer.accepts = [{arg: 'container', type: 'string'}];
MongoStorage.prototype.destroyContainer.returns = {};
MongoStorage.prototype.destroyContainer.http = {verb: 'delete', path: '/:container'};

MongoStorage.prototype.upload.shared = true;
MongoStorage.prototype.upload.accepts = [
  {arg: 'container', type: 'string'},
  {arg: 'req', type: 'object', http: {source: 'req'}},
  {arg: 'res', type: 'object', http: {source: 'res'}}
];
MongoStorage.prototype.upload.returns = {arg: 'result', type: 'object'};
MongoStorage.prototype.upload.http = {verb: 'post', path: '/:container/upload'};

MongoStorage.prototype.getFiles.shared = true;
MongoStorage.prototype.getFiles.accepts = [
  {arg: 'container', type: 'string'}
];
MongoStorage.prototype.getFiles.returns = {arg: 'file', type: 'array', root: true};
MongoStorage.prototype.getFiles.http = {verb: 'get', path: '/:container/files'};

MongoStorage.prototype.getFile.shared = true;
MongoStorage.prototype.getFile.accepts = [
  {arg: 'container', type: 'string'},
  {arg: 'file', type: 'string'}
];
MongoStorage.prototype.getFile.returns = {arg: 'file', type: 'object', root: true};
MongoStorage.prototype.getFile.http = {verb: 'get', path: '/:container/files/:file'};

MongoStorage.prototype.removeFile.shared = true;
MongoStorage.prototype.removeFile.accepts = [
  {arg: 'container', type: 'string'},
  {arg: 'file', type: 'string'}
];
MongoStorage.prototype.removeFile.returns = {};
MongoStorage.prototype.removeFile.http = {verb: 'delete', path: '/:container/files/:file'};

MongoStorage.prototype.download.shared = true;
MongoStorage.prototype.download.accepts = [
  {arg: 'container', type: 'string'},
  {arg: 'file', type: 'string'},
  {arg: 'res', type: 'object', http: {source: 'res'}}
];
MongoStorage.prototype.download.http = {verb: 'get', path: '/:container/download/:file'};

export function initialize(dataSource, callback) {
  let settings = dataSource.settings || {};
  let connector = new MongoStorage(settings);
  dataSource.connector = connector;
  dataSource.connector.dataSource = dataSource;
  connector.DataAccessObject = function() {  };
  for (let m in MongoStorage.prototype) {
    let method = MongoStorage.prototype[m];
    if (_.isFunction(method)) {
      connector.DataAccessObject[m] = method.bind(connector);
      for (let k in method) {
        let opt = method[k];
        connector.DataAccessObject[m][k] = opt;
      }
    }
  }
  connector.define = function(model, properties, settings) {  };
  if (callback) {
    dataSource.connector.connect(callback);
  }
}
