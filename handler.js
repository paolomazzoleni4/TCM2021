const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.get_watch_next = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2)); // log
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.idx) {
        callback(null, {
                    statusCode: 500, // errore
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. idx is null.'
        })
    }
    connect_to_db().then(() => {
        console.log('=> get_all talks, idx: '+ body.idx);
        talk.find({_id:body.idx}) 
            .then(talks => {
                    console.log('watch next: '+ talks[0].watch_next);
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(talks[0].watch_next)
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.Error:'+err
                })
            );
    });
};