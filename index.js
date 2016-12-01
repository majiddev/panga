var express = require('express');
var app = express();
var parser  = require('body-parser');

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


app.get('/', function (req, res) {
  res.send('Hello World!');
});



initIPAdress(); //Setup IP adress before app.listen()

app.listen(port, ipaddress, function() {
        console.log('%s: Node server started on %s:%d ...',
                        Date(Date.now() ), ipaddress, port);
});