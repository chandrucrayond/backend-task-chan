const fs = require('fs');
const csv = require('csv-parser');
const { Pool } = require('pg');

// PostgreSQL pool setup
const pool = new Pool({
    user: 'chandru', // Replace with your PostgreSQL username
    host: 'localhost',
    database: 'postgres', // Replace with your database name
    password: 'chandru', // Replace with your PostgreSQL password
    port: 5432,
});

// Function to create table
export const createTable = async (columns) => {
    try {
        const createTableQuery = `CREATE TABLE IF NOT EXISTS books (${columns.map(col => `${col} TEXT`).join(", ")});`;
        await pool.query(createTableQuery);
        console.log('Table created successfully');
    } catch (err) {
        console.error('Error creating table:', err);
    }
};

// Function to insert data
export const insertData = async (data) => {
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
