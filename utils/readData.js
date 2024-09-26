const express = require('express');
const fs = require('fs');
const csv = require('csv-parser');
const { Pool } = require('pg');

// Initialize Express app
const app = express();
const port = 3000;

// PostgreSQL pool setup
const pool = new Pool({
    user: 'your_user', // Replace with your PostgreSQL username
    host: 'localhost',
    database: 'your_db', // Replace with your database name
    password: 'your_password', // Replace with your PostgreSQL password
    port: 5432,
});

// Function to create table
const createTable = async (columns) => {
    try {
        const createTableQuery = `CREATE TABLE IF NOT EXISTS books (${columns.map(col => `${col} TEXT`).join(", ")});`;
        await pool.query(createTableQuery);
        console.log('Table created successfully');
    } catch (err) {
        console.error('Error creating table:', err);
    }
};

// Function to insert data
const insertData = async (data) => {
    try {
        const columns = Object.keys(data);
        const values = Object.values(data).map(value => `'${value}'`);
        const insertQuery = `INSERT INTO books (${columns.join(", ")}) VALUES (${values.join(", ")});`;
        await pool.query(insertQuery);
        console.log('Data inserted successfully');
    } catch (err) {
        console.error('Error inserting data:', err);
    }
};

// Route to upload and process CSV
app.post('/upload', (req, res) => {
    const filePath = '/mnt/data/Sample book data - nithya, balaji sr - books.csv'; // Path to the CSV file
    let columns = [];

    fs.createReadStream(filePath)
        .pipe(csv())
        .on('headers', (headers) => {
            columns = headers;
            createTable(columns);
        })
        .on('data', (row) => {
            insertData(row);
        })
        .on('end', () => {
            res.send('CSV file processed and data stored in PostgreSQL');
        })
        .on('error', (error) => {
            console.error('Error reading CSV file:', error);
            res.status(500).send('Error processing CSV file');
        });
});

// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
