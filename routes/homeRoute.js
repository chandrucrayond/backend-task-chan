const express = require('express');
const router = express.Router();
const homeController = require('../controllers/homeController');

router.get('/', homeController.getHomeData);

router.get('/id/:id', homeController.getIdData);

module.exports = router;
