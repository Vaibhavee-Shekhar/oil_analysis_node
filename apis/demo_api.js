const express = require('express');
const fs = require('fs');
const { connectToDatabase } = require('./connect3.js'); // Your database connection module
const ExcelJS = require('exceljs');

const router = express.Router();

// Set up CORS middleware
router.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.header('Access-Control-Allow-Credentials', 'true');
    res.header('Access-Control-Max-Age', '3600');
    res.header('Access-control-Max-State,')
    next();
});









module.exports = router;