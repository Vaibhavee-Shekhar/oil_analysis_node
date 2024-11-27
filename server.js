const express = require('express');
const app = express();
const port = 2999;
// const port = process.env.PORT || 3000;


// Import routes
const api1 = require('./apis/demo_api');





// Use routes with a dynamic router prefix
app.use('/api/demo_api', api1);