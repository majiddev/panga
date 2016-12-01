var express = require('express');
var fs = require('fs');
/*
var crawledFile = 'data/crawled.txt';
var urlFile = 'data/urls.txt';
var errorsFile = 'data/errors.txt';

var urlWriter = fs.createWriteStream(urlFile, {
    flags: 'a'
});
var errorWriter = fs.createWriteStream(errorsFile, {
    flags: 'a'
});
var crawlerWriter = fs.createWriteStream(crawledFile, {
    flags: 'a'
});
*/
var gitLastTime = new Date();

var request = require('request');
var cheerio = require('cheerio');
var app     = express();
var path = require('path');

var domainNameBase = '//perfectionservo.com';
var domainNameBaseWWW = '//www.perfectionservo.com';
var allUrls = new Array();
var crawledUrls = new Array();
var errorUrls = new Array();
var saveUrls = new Array();
var hitCounter = {};
var foundUrlId = '1upSiGTa1J';
var crawledUrlId = 'GG5Qk6qenD';
var errorUrlId = 'GTviCTt5JX';
var lastHitTime = new Date();
var crawlerStoped = true;
var restartCounter = 0;

var bodyParser = require('body-parser');
var ParseServer = require('parse-server').ParseServer;
var Parse = require('parse/node');

var appId = process.env.APP_ID || 'oii2o34io234i23u4o23u4o23';
var masterKey = process.env.MASTER_KEY || 'ajsuidha87sy788ka09d*&*^&asjdioajsdo';
var serverURL = process.env.SERVER_URL || 'https://bambudan.herokuapp.com/parse';

var api = new ParseServer({
  databaseURI: 'mongodb://dbuser:123456789@ds035816.mlab.com:35816/pangaapp',
  appId: appId,
  masterKey: masterKey,
  serverURL: serverURL,
  cloud: process.env.CLOUD_CODE_MAIN || __dirname + '/cloud/main.js',
  clientKey: process.env.CLIENT_KEY || 'a65s4d5as4d65as4d6a4s6d5aasd6',
  appName: process.env.APP_NAME || 'Majid APP for url crawling',
  publicServerURL : process.env.PUBLIC_SERVER_URL || 'https://bambudan.herokuapp.com/parse',
      liveQuery: {
        classNames: ["Posts", "Comments"] // List of classes to support for query subscriptions
      },
      allowClientClassCreation: process.env.CLIENT_CLASS_CREATION || false, // <<< This line is added for disabling client class creation
      enableAnonymousUsers : process.env.ANONYMOUS_USERS || true
});

Parse.initialize(appId, masterKey);
Parse.serverURL = serverURL;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));


//Setup ip adress and port
var ipaddress ;

function initIPAdress() {
    var adr = process.env.OPENSHIFT_NODEJS_IP;
    if (typeof adr === "undefined") {
            //  Log errors on OpenShift but continue w/ 127.0.0.1 - this
            //  allows us to run/test the app locally.
            console.warn('No OPENSHIFT_NODEJS_IP var, using localhost');
            adr = 'localhost';
    }

    ipaddress = adr;
}

var port      = process.env.OPENSHIFT_NODEJS_PORT || 8080;
initIPAdress(); //Setup IP adress before app.listen()


var httpServer = require('http').createServer(app);
httpServer.listen(port, ipaddress, function() {
        console.log('%s: Node server started on %s:%d ...',
                        Date(Date.now() ), ipaddress, port);
});
httpServer.timeout = 36000000;

app.get('/', function(req, res) {
    res.send('Welcome node js url crawler');
});

var mountPath = process.env.PARSE_MOUNT || '/parse';
app.use(mountPath, api);

process.on('SIGTERM', function () {
    console.log('in hook...');
    updateRecords(true);
    httpServer.close(function () {
      process.exit(10);
    });
});

app.get('/resume', function(req, res) {
    checkRestart();
    res.send('Resume process going to start...');
});

/*
function keepupdated(){
    console.log('keepupdated');
    if( crawlerStoped && restartCounter < 3 ){
        console.log('keepupdated 1');
        restartCounter+=1;
        setTimeout(function(){  
            console.log('keepupdated 2');
            keepupdated();
        }, 300000);
        return true;
    }
    else if( crawlerStoped && restartCounter >= 3 && restartCounter < 6 ){
        //checkRestart();
        return true;
    }
    else if( crawlerStoped && restartCounter >= 6 ){
        console.log('Check done 6-times, we are OK');
        httpServer.close(function () {
            process.exit(0);
        });
        return true;
    }
    
    setTimeout(function(){  
        keepupdated();
    }, 300000);
}
*/

function checkRestart(){
    var URLsClass = Parse.Object.extend("perfection_urls");
    var urlsQuery = new Parse.Query(URLsClass);
    var content = '';
    urlsQuery.get( errorUrlId, {
        success: function(urlsObject) {
            var urls = urlsObject.get('urls');
            if( urls.length > 0 ){
                restartCrawler(urls);
            }
        },
        error: function(object, error) {
            console.log('No Error URL Found. We are done.');
        }
    });
}

function restartCrawler(urls){
    var errorUrlsPending = new Array();
    for( var i=0; i<urls.length; i++ ){
        var curUrl = urls[i];
        if( curUrl && errorUrlsPending.indexOf(curUrl)  === -1 ){   //  remove duplication
            errorUrlsPending.push(curUrl);
        }
    }
    
    errorUrls = errorUrlsPending;
    
    var URLsClass = Parse.Object.extend("perfection_urls");
    var urlsQuery = new Parse.Query(URLsClass);
    urlsQuery.get( foundUrlId, {
        success: function(urlsObject) {
            var urls = urlsObject.get('urls');
            if( urls.length > 0 ){
                reloadFoundUrls(urls);
            }
        },
        error: function(object, error) {
            console.log('Founded URLs missed to restart.');
        }
    });
    
    var URLsClass = Parse.Object.extend("perfection_urls");
    var urlsQuery = new Parse.Query(URLsClass);
    urlsQuery.get( crawledUrlId, {
        success: function(urlsObject) {
            var urls = urlsObject.get('urls');
            if( urls.length > 0 ){
                reloadCrawledUrls(urls);
            }
        },
        error: function(object, error) {
            console.log('Crawled URLs missed to restart.');
        }
    });
    
    console.log('Wait 1-minute, data is being ready...');
    setTimeout(function(){  
        for( var i=0; i < errorUrls.length; i++ ){
            var errorUrl = errorUrls[i];
            if( errorUrl ){
                saveCrawling(errorUrl);
            }
        }
    }, 60000);
    
    
    
    //restartCounter = 0;
    //keepupdated();
    /*
    for( var i=0; i < urlsLength; i++ ){
        var errorUrl = errorUrls[i];
        if( errorUrl ){
            saveCrawling(errorUrl);
        }
    }
    */
}

function reloadFoundUrls(urls){
    allUrls = new Array();
    for( var i=0; i<urls.length; i++ ){
        var curUrl = urls[i];
        if( curUrl && allUrls.indexOf(curUrl) === -1 ){
            allUrls.push(curUrl);
        }
    }    
}

function reloadCrawledUrls(urls){
    crawledUrls = new Array();
    for( var i=0; i<urls.length; i++ ){
        var curUrl = urls[i];
        if( curUrl && crawledUrls.indexOf(curUrl) === -1 ){
            crawledUrls.push(curUrl);
        }
    }
}

app.get('/reset', function(req, res) {
//    fs.writeFile(urlFile, '');
    setTimeout(function(){  
        processParse( foundUrlId, null );
    }, 10000);
    setTimeout(function(){  
        processParse( crawledUrlId, null );
    }, 10000);
    setTimeout(function(){  
        processParse( errorUrlId, null );
    }, 10000);
    res.send('All records have been reset!');
});

function updateRecords(instant){
    if( instant ){
        if( allUrls.length > 0 ){
            allUrls.sort();
            processParse( foundUrlId, allUrls );
        }
        if( crawledUrls.length > 0 ){
            crawledUrls.sort();
            processParse( crawledUrlId, crawledUrls );
        }
        if( errorUrls.length > 0 ){
            errorUrls.sort();
            processParse( errorUrlId, errorUrls );
        }
        return true;
    }
    
    var curDateTime = new Date();
    var seconds = (curDateTime.getTime() - lastHitTime.getTime()) / 1000;
    var minutes = seconds/60;
    if( minutes < 5 ){
        return true;
    }
    
    if( allUrls.length > 0 ){
        allUrls.sort();
        setTimeout(function(){
            processParse( foundUrlId, allUrls );
        }, 20000);
    }
    if( crawledUrls.length > 0 ){
        crawledUrls.sort();
        setTimeout(function(){
            processParse( crawledUrlId, crawledUrls );
        }, 20000);
    }
    if( errorUrls.length > 0 ){
        errorUrls.sort();
        setTimeout(function(){
            processParse( errorUrlId, errorUrls );
        }, 20000);
    }
    
    //  Reset last update time
    lastHitTime = new Date();
}

function processParse(id, data){
    var URLsClass = Parse.Object.extend("perfection_urls");
    var urlsQuery = new Parse.Query(URLsClass);
    
    urlsQuery.get(id, {
        success: function(urlsObject) {
            urlsObject.set("urls", data);
            urlsObject.save();
        },
        error: function(object, error) {
            console.log('Parse error:', error);
        }
    });
}

app.get('/test', function(req, res) {
    
    res.send('Testing...');
    
    
});

app.get('/urls', function(req, res) {
    var URLsClass = Parse.Object.extend("perfection_urls");
    var urlsQuery = new Parse.Query(URLsClass);
    var content = '';
    urlsQuery.get( foundUrlId, {
        success: function(urlsObject) {
            //var urlsObj = urlsObject[0];
            var urls = urlsObject.get('urls');
            if( urls.length > 0 ){
                content = urls.join('\n');
                res.send(content);
            }
        },
        error: function(object, error) {
            res.send('URLs not available!');
        }
    });
});

app.get('/errors', function(req, res) {
    var URLsClass = Parse.Object.extend("perfection_urls");
    var urlsQuery = new Parse.Query(URLsClass);
    var content = '';
    urlsQuery.get( errorUrlId, {
        success: function(urlsObject) {
            var urls = urlsObject.get('urls');
            if( urls.length > 0 ){
                content = urls.join('\n');
                res.send(content);
            }
        },
        error: function(object, error) {
            res.send('URLs not available!');
        }
    });
});

app.get('/crawled', function(req, res) {
    var URLsClass = Parse.Object.extend("perfection_urls");
    var urlsQuery = new Parse.Query(URLsClass);
    var content = '';
    urlsQuery.get( crawledUrlId, {
        success: function(urlsObject) {
            var urls = urlsObject.get('urls');
            if( urls.length > 0 ){
                content = urls.join('\n');
                res.send(content);
            }
        },
        error: function(object, error) {
            res.send('URLs not available!');
        }
    });
});

app.get('/scrape', function(req, res){
    
    var url = 'http://www.perfectionservo.com';
    saveCrawling(url);

    res.send('Crawling started...');
  
});


function unsetErrorUrl(url){
    for (var key in errorUrls) {
        if (errorUrls[key] === url) {
            errorUrls.splice(key, 1);
        }
    }
}

function addErrorUrl(url){
    if( errorUrls.indexOf(url) < 0 ){
        errorUrls.push(url);
    }
}

function addCrawledUrl(url){
    if( crawledUrls.indexOf(url) < 0 ){
        crawledUrls.push(url);
    }
}

function addFoundUrl(url){
    if( allUrls.indexOf(url) < 0 ){
        allUrls.push(url);
    }    
}


function saveCrawling(url){
    setTimeout(function(){
        continueCrawling( url );
    }, 120000);
}

function continueCrawling( url, retry ){
    console.log('Continue crawling for ... '+url);
    request(url, function (error, response, html) {
        if (!error && response.statusCode === 200) {
            crawlerStoped = false;
            console.log('Request sent...');
            var $ = cheerio.load(html);
            var linksLength = $('a').length;
            if( linksLength < 1 ){
                console.log('Links not found...');
                addErrorUrl(url,'___links not found');
                addCrawledUrl(url);
            }
            else{
                console.log(linksLength+' Found links, Continue collection...');
                addCrawledUrl(url);
                //  Collect all valid URLs on current URL
                var curFoundUrls = new Array();
                $('a').each(function(i, element){
                    var a = $(this);
                    var href = a.attr('href');
                    if( typeof href !== 'undefined'  ){
                        var validatedUrl = validateURL( href );
                        if( validatedUrl !== false && validatedUrl !== '' ){
                            if( allUrls.indexOf(validatedUrl) < 0 ){
                                addFoundUrl(validatedUrl);
                                console.log('Saved URL: '+validatedUrl);
                            }
                            if( validatedUrl.indexOf('?tag=') > -1 ){   //  URL has fatal errors, can not be crawled
                                addFoundUrl(validatedUrl);
                                addCrawledUrl(validatedUrl);
                            }
                            if( crawledUrls.indexOf(validatedUrl) < 0 && curFoundUrls.indexOf(validatedUrl) < 0 ){
                                curFoundUrls.push(validatedUrl);
                            } 
                        }

                    }
                });
                
                //  Remove entry from error array
                if( errorUrls.indexOf(url) > -1 ){
                    unsetErrorUrl(url);
                }
                
                var curFoundLength = curFoundUrls.length;
                //  Continue crawling for found URLs
                if( curFoundLength > 0 ){
                    console.log('URLs saved for ... '+url);
                    for(var i=0; i<curFoundLength; i++){
                        var curUrl = curFoundUrls[i];
                        saveCrawling(curUrl);
                    }
                }
                
            }
        }
        else{
            console.log('error url:', url);
            if( hitCounter[url] === 10 ){
                var errorNo = '';
                if(error){
                    errorNo = ( error.errno ) ? '>>>'+error.errno : '';
                }
                addErrorUrl(url);
                console.log('Error Logged url:', errorNo);
            }
            else if( ! hitCounter[url] || hitCounter[url] < 10 ){
                hitCounter[url] = ( hitCounter[url] >= 0 ) ? hitCounter[url]+=1 : 1;
                console.log('Error Retry '+hitCounter[url]+':', url);
                setTimeout(function(){
                    saveCrawling(url, true);
                }, 120000);
            }
        }
        //console.log('Hit Counter...');
        //console.log(hitCounter);
    });
    
    crawlerStoped = true;
    
    //  update on parse
    updateRecords();
    
}


function validateURL( url ){
    if( url.indexOf('javascript:') > -1 ){
        return false;
    }
    if( typeof url === 'undefined'  ){
        return false;
    }
    if( ! allowedExt( url ) ){
        return false;
    }
    if( url === '/' ){
        return false;
    }
    if( url.indexOf('mailto:') > -1 ){
        return false;
    }
    var validReq = true;
    
    var verifyDomain = checkSetDomain( url );
    if( verifyDomain ){
        var finalUrl = cleanHash( verifyDomain.trim() );
        request(finalUrl, function (error, response, html) {
            if (error || ( typeof response !== 'undefined' && response.statusCode !== 200 ) ) {
                validReq = false;
            }  
        });
        if( ! validReq ){
            return false;
        }
        
        return finalUrl;
    }
    return false;
}

function allowedExt( url ){
    if( url.lastIndexOf( '.' ) > -1 ){
        var filesExt = ['pdf', 'png', 'jpg', 'jpeg', 'gif', 'bmp', 'zip', 'rar', 'tar', 'doc', 'docx', 'xls', 'xlsx'];
        var extArr = url.split('.');
        var ext = extArr[extArr.length - 1].toLowerCase();
        if( filesExt.lastIndexOf(ext) < 0 ){
            return url;
        }
        return false;
    }
    return url;
}



function checkSetDomain( url ){
    if( typeof url === 'undefined'  ){
        return false;
    }
    var domainPos = url.indexOf(domainNameBase);
    var domainPosWWW = url.indexOf(domainNameBaseWWW);
    //  If url has local domain
    if( domainPos > -1 || domainPosWWW > -1 ){
        var httpPos = url.indexOf('http:');
        var httpsPos = url.indexOf('https:');
        return ( httpPos > -1 || httpsPos > -1 ) ? setWWW(url) : setWWW('http:'+url);                    
    }

    //  Check for other domain URL
    var httpPos = url.indexOf('http:');
    var httpsPos = url.indexOf('https:');
    var wwwPos = url.indexOf('//www.');

    if( httpPos > -1 || httpsPos > -1 || wwwPos > -1 ){ //  URL belongs to other domain
        return false;
    }
    
    //  If there is no slash prefix
    var firstChar = url.charAt(0);
    url = ( firstChar === '/' ) ? url : '/'+url;
    
    //  URL belongs to local domain, but do not has domain prefix, so add prefix
    return setWWW('http:'+domainNameBaseWWW+url);
}

function setWWW( url ){
    var finalUrl = '';
    if( url.indexOf('//www.') > -1 ){
        return url;
    }
    if( url.indexOf('http://') > -1 ){
        var urlArr = url.split('http://');
        finalUrl = 'http://www.'+urlArr[1];
    }
    else if( url.indexOf('https://') > -1 ){
        var urlArr = url.split('https://');
        finalUrl = 'https://www.'+urlArr[1];
    }
    if( finalUrl !== '' ){
        return finalUrl;
    }
    return url;
}

function cleanHash( url ){
    var hashPos = url.indexOf('#');
    if( hashPos > -1 ){
        var hashArr = url.split('#');
        return hashArr[0];
    }
    return url;
}