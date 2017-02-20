import { expect } from 'chai';
import fs from 'fs';
import Grid from 'gridfs-stream';
import { GridStore } from 'mongodb';
import { ObjectID } from 'mongodb';
import loopback from 'loopback';
import mongo from 'mongodb';
import path from 'path';
import StorageService from '../source';
import request from 'supertest';

let insertTestFile = function(ds, container, done) {
  let options = {
    filename: 'item.png',
    mode: 'w',
    metadata: {
      'mongo-storage': true,
      container,
      filename: 'item.png'
    }
  };
  let gfs = Grid(ds.connector.db, mongo);
  let write = gfs.createWriteStream(options);
  let read = fs.createReadStream(path.join(__dirname, 'files', 'item.png'));
  read.pipe(write);
  return write.on('close', () => done());
};

describe('mongo gridfs connector', function() {

  let agent       = null;
  let app         = null;
  let datasource  = null;
  let server      = null;

  describe('datasource', function() {

    it('should exist', () => expect(StorageService).to.exist);

    return describe('default configuration', function() {

      before(function(done) {
        datasource = loopback.createDataSource({
          connector: StorageService,
          hostname: '127.0.0.1',
          port: 27017
        });
        return setTimeout(done, 200);
      });
      
      it('should create the datasource', function() {
        expect(datasource).to.exist;
        expect(datasource.connector).to.exist;
        return expect(datasource.settings).to.exist;
      });

      it('should create the url', function() {
        expect(datasource.settings.url).to.exist;
        return expect(datasource.settings.url).to.eql("mongodb://127.0.0.1:27017/test");
      });

      return it('should be connected', () => expect(datasource.connected).to.eql(true));
    });
  });

  describe('model usage', function() {

    let model = null;

    before(function(done) {
      datasource = loopback.createDataSource({
        connector: StorageService,
        hostname: '127.0.0.1',
        port: 27017
      });
      model = datasource.createModel('MyModel');
      return setTimeout(done, 200);
    });

    it('should create the model', () => expect(model).to.exist);

    describe('getContainers function', function() {

      it('should exist', () => expect(model.getContainers).to.exist);

      return it('should return an empty list', done =>
        model.getContainers(function(err, list) {
          expect(Array.isArray(list)).to.eql(true);
          return done();
        })
      );
    });

    describe('getContainer function', () =>

      it('should exist', () => expect(model.getContainer).to.exist)
    );

    return describe('upload function', () =>

      it('should exist', () => expect(model.upload).to.exist)
    );
  });

  return describe('application usage', function() {

    app     = null;
    let ds      = null;
    server  = null;
    
    before(function(done) {
      app = loopback();
      app.set('port', 5000);
      app.set('url', '127.0.0.1');
      app.set('legacyExplorer', false);
      app.use(loopback.rest());
      ds = loopback.createDataSource({
        connector: StorageService,
        hostname: '127.0.0.1',
        port: 27017
      });
      let model = ds.createModel('MyModel', {}, {
        base: 'Model',
        plural: 'my-model'
      }
      );
      app.model(model);
      return setTimeout(done, 200);
    });

    before(done => ds.connector.db.collection('fs.files').remove({}, done));

    before(done => server = app.listen(done));

    after(() => server.close());

    describe('getContainers', function() {

      describe('without data', () =>

        it('should return an array', done =>
          request('http://127.0.0.1:5000')
          .get('/my-model')
          .end(function(err, res) {
            expect(res.status).to.equal(200);
            expect(Array.isArray(res.body)).to.equal(true);
            expect(res.body.length).to.equal(0);
            return done();
          })
        )
      );

      return describe('with data', function() {

        before(done => insertTestFile(ds, 'my-cats', done));

        return it('should return an array', done =>
          request('http://127.0.0.1:5000')
          .get('/my-model')
          .end(function(err, res) {
            expect(res.status).to.equal(200);
            expect(Array.isArray(res.body)).to.equal(true);
            expect(res.body.length).to.equal(1);
            expect(res.body[0].container).to.equal('my-cats');
            return done();
          })
        );
      });
    });

    describe('getContainer', function() {

      describe('without data', () =>

        it('should return an array', done =>
          request('http://127.0.0.1:5000')
          .get('/my-model/fake-container')
          .end(function(err, res) {
            expect(res.status).to.equal(200);
            expect(res.body.container).to.equal('fake-container');
            expect(Array.isArray(res.body.files)).to.equal(true);
            expect(res.body.files.length).to.equal(0);
            return done();
          })
        )
      );

      return describe('with data', function() {

        before(done => insertTestFile(ds, 'my-cats-1', done));

        return it('should return an array', done =>
          request('http://127.0.0.1:5000')
          .get('/my-model/my-cats-1')
          .end(function(err, res) {
            expect(res.status).to.equal(200);
            expect(res.body.container).to.equal('my-cats-1');
            expect(Array.isArray(res.body.files)).to.equal(true);
            expect(res.body.files.length).to.equal(1);
            return done();
          })
        );
      });
    });

    describe('upload', () =>

      it('should return 20x', done =>
        request('http://127.0.0.1:5000')
        .post('/my-model/my-cats/upload')
        .attach('file', path.join(__dirname, 'files', 'item.png'))
        .end(function(err, res) {
          expect(res.status).to.equal(200);
          return done();
        })
      )
    );

    describe('download', function() {
   
      before(done => ds.connector.db.collection('fs.files').remove({}, done));
     
      before(done => insertTestFile(ds, 'my-cats', done));

      return it('should return the file', done =>
        request('http://127.0.0.1:5000')
        .get('/my-model/my-cats/download/item.png')
        .end(function(err, res) {
          expect(res.status).to.equal(200);
          return done();
        })
      );
    });

    describe('removeFile', function() {
 
      before(done => ds.connector.db.collection('fs.files').remove({}, done));
     
      before(done => insertTestFile(ds, 'my-cats', done));

      return it('should return the file', done =>
        request('http://127.0.0.1:5000')
        .delete('/my-model/my-cats/files/item.png')
        .end(function(err, res) {
          expect(res.status).to.equal(200);
          return done();
        })
      );
    });

    return describe('destroyContainer', function() {
 
      before(done => ds.connector.db.collection('fs.files').remove({}, done));
     
      before(done => insertTestFile(ds, 'my-cats', done));

      return it('should return the file', done =>
        request('http://127.0.0.1:5000')
        .delete('/my-model/my-cats')
        .end(function(err, res) {
          expect(res.status).to.equal(200);
          return done();
        })
      );
    });
  });
});
