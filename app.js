// app.js

require('dotenv').config();
const express = require('express');
const morgan = require('morgan');
const bodyParser = require('body-parser');
const cors = require('cors');
const exampleRoute = require('./routes/exampleRoute');
const homeRoute = require('./routes/homeRoute');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(morgan('dev'));         // Logging
app.use(cors());                // Enable CORS
app.use(bodyParser.json());     // Parse JSON bodies

// Routes
app.use('/api/example', exampleRoute);

app.use('/', homeRoute);


// Catch-all error handler
app.use((err, req, res, next) => {
    console.error(err.message);
    res.status(500).json({ error: 'Internal server error' });
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});