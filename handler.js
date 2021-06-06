const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.add_comment = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2)); // log
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.id_user) {
        callback(null, {
                    statusCode: 500, // errore
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. id_user is null.'
        })
    }
    
    if(!body.talk_id) {
        callback(null, {
                    statusCode: 500, // errore
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks. talk_id is null.'
        })
    }
    
    if (!body.comment) {
        body.comment = " "
    }
    
    var comment = {
        id_user : body.id_user,
        comment : body.comment
    }
    
    connect_to_db().then(() => {
        console.log('=> get_all talks');
        talk.findOne({_id:body.talk_id}, function(err,foundTalk){
            foundTalk.comments.push(comment);
            foundTalk.save(function(err,foundTalk) {
                if(err) throw err;
                callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(foundTalk)
                    })
            })
        })
        .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.Error:'+err
                })
            );
    });
};