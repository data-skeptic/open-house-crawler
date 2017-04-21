var AWS = require('aws-sdk')
var ses = new AWS.SES()
 
var RECEIVER = 'kylepolich@gmail.com'
var SENDER = 'kylepolich@gmail.com'
 
exports.handler = function (event, context) {
    console.log('Received event:', event)
    sendEmail(event, function (err, data) {
        context.done(err, null)
    })
}
 
function sendEmail (event, done) {
    var params = {
        Destination: {
            ToAddresses: [
                RECEIVER
            ]
        },
        Message: {
            Body: {
                Text: {
                    Data: 'Name: ' + event.name + '\nEmail: ' + event.email + '\nDesc: ' + event.description,
                    Charset: 'UTF-8'
                }
            },
            Subject: {
                Data: 'Website Referral Form: ' + event.name,
                Charset: 'UTF-8'
            }
        },
        Source: SENDER
    }
    ses.sendEmail(params, done)
}