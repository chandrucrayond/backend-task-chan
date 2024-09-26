// routes/exampleRoute.js

const express = require('express');
const router = express.Router();
const exampleController = require('../controllers/exampleController');

router.get('/', exampleController.getExampleData);

module.exports = router;
