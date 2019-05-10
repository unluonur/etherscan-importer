var fs = require('fs');
var path = require('path');
var readline = require('readline');
var request = require('request');
var async = require('async');


var config = require('./config');


class GetLogs {
    constructor() {
        this.fieldNames = [
            'id',
            'batchNo',
            'address',
            'topics',
            'data',
            'blockNumber',
            'gasPrice',
            'gasUsed',
            'logIndex',
            'transactionHash',
            'transactionIndex',
            'timeStamp',
            'date'
        ];
    }

    run(cb) {
        var self = this;
        this.currentBlock = config.getLogs.fromBlock;
        var fileName = config.getLogs.fileName;
        var dirName = path.dirname(config.getLogs.fileName);
        var batchFileName = path.join(dirName, path.basename(fileName, path.extname(fileName))+".batch.txt");
        fs.mkdirSync(dirName, {recursive: true});

        this.batch = {};

        var doFn = () => {

            var tasks = [];
            for(var i=0; i<config.parallelism; i++) {
                for(var j=0; j<config.getLogs['api-keys'].length; j++) {
                    tasks.push(((apiKey) => {
                        return (cb) => {
                            self.runThread(apiKey, (err, count) => {
                                cb(err, count);
                            })
                        };
                    })(config.getLogs['api-keys'][j]));
                }
            }
            async.parallel(tasks, (err, results) => {
                self.file.close();
                self.batchFile.close();
                if(err) {
                    return cb(err)
                }
                var total = 0;
                for(var i=0; i<results.length; i++) {
                    total += results[i]
                }
                cb(null, total);
            });
        }

        if(fs.existsSync(fileName) && fs.existsSync(batchFileName)) {
            var rl = readline.createInterface({
                input: fs.createReadStream(batchFileName)
            });

            rl.on('line', function(line) {
                self.batch[line] = true;
            });

            rl.on('close', function() {
                self.file = fs.createWriteStream(fileName, {flags: 'a'});
                self.batchFile = fs.createWriteStream(batchFileName, {flags: 'a'});
                doFn();
            });
        }
        else {
            this.file = fs.createWriteStream(fileName, {flags: 'w'});
            this.batchFile = fs.createWriteStream(batchFileName, {flags: 'w'});
            this.file.on('ready', () => {
                self.file.write(self.fieldNames.join(',')+'\n');
                doFn();
            })
            this.file.on('error', (err) => {
                console.log(err.message)
            })

        }
    }

    runThread(apiKey, cb) {
        var self = this;

        var batchSize = config.getLogs['batch-size'];

        var loop = (cb) => {

            var fromBlock = self.currentBlock;
            self.currentBlock += batchSize;

            var doFn = (fromBlock, cb) => {
                console.log('fetching from block : '+fromBlock)

                if(self.batch[self.currentBlock]) {
                    console.log('block '+fromBlock+' has already been downloaded')
                    return cb(null, batchSize);
                }

                var url = config['api-url'] + '?module=logs&action=getLogs&fromBlock='+fromBlock+'&toBlock=latest&address='+config.getLogs['address']+'&apiKey='+apiKey;
                request(url, {"json": true}, (err, res, body) => {
                    if(!err && !body.result) {
                        err = new Error('!! Rate Limit !!');
                    }
                    if(err) {
                        return cb(err);
                    }
                    if(body.result.length > batchSize) {
                        return cb(new Error('!! length error : '+body.result.length));
                    }
                    if(body.result.length == batchSize) {
                        for(var i=0; i<body.result.length; i++) {
                            var obj = body.result[i];
                            var arr = [];
                            arr.push(fromBlock + i);
                            arr.push(fromBlock);
                            arr.push(obj['address']);
                            arr.push(obj['topics'].join('|'));
                            arr.push(obj['data']);
                            arr.push(parseInt(obj['blockNumber']));
                            arr.push(parseInt(obj['gasPrice']));
                            arr.push(parseInt(obj['gasUsed']));
                            arr.push(parseInt(obj['logIndex']));
                            arr.push(obj['transactionHash']);
                            arr.push(parseInt(obj['transactionIndex']));
                            arr.push(parseInt(obj['timeStamp']));
                            arr.push(new Date(parseInt(obj['timeStamp']) * 1000).toISOString());
                            self.file.write(arr.join(',') + '\n');
                        }
                        self.batchFile.write(fromBlock+'\n');
                    }
                    cb(null, body.result.length);
                });
            }

            var retryCount = config['retry-count'];

            var startTime = new Date().getTime()
            var callback = (err, count) => {
                if(err) {
                    if(retryCount > 1) {
                        retryCount--;
                        console.log('retrying for block '+fromBlock+'. '+retryCount+' times left')
                        return setTimeout(() => {
                            doFn(fromBlock, callback);
                        }, Math.random() * config['retry-ms'])
                    }
                    console.log('retried but failed for block '+fromBlock)
                    return cb(err);
                }
                console.log('block '+fromBlock+' in '+(new Date() - startTime)+' ms')
                return cb(null, count);
            }

            return doFn(fromBlock, callback);
        }

        var total = 0;

        var callback = (err, count) => {
            if(err) {
                return cb(err);
            }
            if(count < batchSize) {
                return cb(null, total);
            }
            total += count;
            return loop(callback);
        }

        loop(callback);

    }

}

var task = new GetLogs();
var startTime = new Date().getTime();
task.run((err, count) => {
    if(err) {
        console.log('COMPLETED WITH ERROR : '+err.message);
    }
    else {
        console.log('SUCCESS!')
        console.log(''+count+' records')
    }
    console.log(''+(new Date().getTime() - startTime)+' ms')
})

