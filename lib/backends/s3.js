var _ = require('lodash');
var Q = require('q');
var util = require('util');
var AWS = require('aws-sdk');

var Backend = require('./backend');

function S3Backend() {
    var that = this;
    Backend.apply(this, arguments);

    this.opts = _.defaults(this.opts || {}, {
        proxyAssets: true,
        channels : ['stable', 'beta', 'alpha']
    });

    if (!this.opts.credentials.aws.accessKeyId || !this.opts.credentials.aws.secretAccessKey || !this.opts.configuration.aws.bucket) {
        throw new Error('S3 backend requires "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", and "AWS_BUCKET" options');
    }

    AWS.config.accessKeyId = this.opts.credentials.aws.accessKeyId;
    AWS.config.secretAccessKey = this.opts.credentials.aws.secretAccessKey;
    this.client = new AWS.S3({
        signatureVersion: 'v4',
        region: 'eu-west-3'
    });
    this.releases = this.memoize(this._releases);
}

util.inherits(S3Backend, Backend);

// List all releases for this repository
S3Backend.prototype._releases = function() {
    var client = this.client;
    var bucket = this.opts.configuration.aws.bucket;

    var promises = this.opts.channels.map(function (channel){
        var deferRelease = Q.defer();
        var params = {
            Bucket: bucket,
            Delimiter: '/',
            Prefix : channel + '/'
        }

        client.listObjects(params, function (err, data) {
            if (err) deferRelease.reject(err);
            var folders = data.CommonPrefixes.map(function (commonPrefix) {
                var deferFolder = Q.defer();
                var folderParams = {
                    Bucket: bucket,
                    Prefix: commonPrefix.Prefix
                }

                client.listObjects(folderParams, function (err, contents) {
                    if (err) deferRelease.reject(err);

                    return deferFolder.resolve(contents);
                });

                return deferFolder.promise;
            });

            Q.all(folders).done(function (values) {
                var releases = values.map(function (release) {
                    var version = release.Prefix.split('/').slice(-2)[0];
                    var date = release.Contents.length ? release.Contents[0].LastModified : new Date;
                    var release = {
                        channel: channel,
                        tag_name : version,
                        published_at : date,
                        assets: _.filter(release.Contents.map(function (content) {
                            var name = content.Key.split('/').slice(-1)[0];
                            return {
                                id: content.ETag,
                                tag_name : version,
                                key: content.Key,
                                name: name,
                                size: content.Size,
                                content_type: 'application/zip',
                            }
                        }), e=>e.name.match(/\.(zip|dmg|nupkg|exe)$/))
                    }
                    return release
                });
                deferRelease.resolve(releases);
            });
        });



        return deferRelease.promise
    })
   

    return Q.all(promises).then(_.flatten)
};

S3Backend.prototype.serveAsset = function(asset, req, res) {
    if (!this.opts.proxyAssets) {
        var s3url = this.client.getSignedUrl('getObject', {
            Bucket: this.opts.configuration.aws.bucket,
            Key: asset.raw.key
        });
        res.redirect(s3url);
    } else {
        return Backend.prototype.serveAsset.apply(this, arguments);
    }
};

// Return stream for an asset
S3Backend.prototype.getAssetStream = function(asset) {
    var params = {
        Bucket: this.opts.configuration.aws.bucket,
        Key: asset.raw.key
    };

    return Q(this.client.getObject(params).createReadStream());
};

module.exports = S3Backend;
