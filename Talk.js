const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    title: String,
    url: String,
    details: String,
    main_author: String,
    watch_next : [{
        watch_next_idx: String,
        url: String,
        title: String
    }]
}, { collection: 'tedx_data', _id: false  });

module.exports = mongoose.model('talk', talk_schema);