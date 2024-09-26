// Route to upload and process CSV
const fs = require("fs");
const csv = require("csv-parser");

const { Pool } = require("pg");

// PostgreSQL pool setup
const pool = new Pool({
  user: "postgres", 
  host: "localhost",
  database: "postgres", 
  password: "chandru", 
  port: 5432,
});

const createTable = async () => {
  try {
    const dropTableQuery = `
     DROP TABLE IF EXISTS books;
   `;
    console.log("Dropping table as it already exists...");
    await pool.query(dropTableQuery);

    const createTableQuery = `
            CREATE TABLE IF NOT EXISTS books (
                   bookID SERIAL PRIMARY KEY,
                   title VARCHAR(255) NOT NULL,
                   authors VARCHAR(255),
                   average_rating NUMERIC(3, 2),
                   isbn VARCHAR(15),
                   isbn13 VARCHAR(20),
                   language_code VARCHAR(10),
                   num_pages INT,
                   ratings_count INT,
                   text_reviews_count INT,
                   publication_date DATE,
                   publisher VARCHAR(255)
         );
        `;
    console.log(createTableQuery);
    await pool.query(createTableQuery);
    console.log("Table created successfully");
  } catch (err) {
    console.error("Error creating table:", err);
  }
};

/// Function to insert data
const insertData = async (data) => {
  try {
    const validationErrors = validateData(data);

    // If validation errors exist, log them and stop the operation
    if (validationErrors.length > 0) {
      console.error("Validation failed with errors:", validationErrors);
      return;
    }

    const columns = Object.keys(data);
    const values = Object.values(data);

    // Ensure there are no empty columns
    if (columns.length === 0) {
      console.error("No columns to insert.");
      return;
    }

    // Constructing placeholders ($1, $2, ...) for parameterized query
    const valuePlaceholders = values.map((_, i) => `$${i + 1}`).join(", ");

    const insertQuery = `INSERT INTO books (${columns.join(
      ", "
    )}) VALUES (${valuePlaceholders})`;

    // Using parameterized query
    await pool.query(insertQuery, values);
    console.log("Data inserted successfully");
  } catch (err) {
    console.log("Error inserting data:", data.bookId);
  }
};


exports.uploadData = (req, res) => {
  const filePath = "./utils/Sample book data.csv"; // Path to the CSV file
  let columns = [];
  let currentRow = 0;

  pool.connect();

  const stream = fs.createReadStream(filePath).pipe(csv());

  stream.on("headers", async (headers) => {
    columns = headers;
    await createTable(columns);
  });

  stream.on("data", async (row) => {
    currentRow++;
    stream.pause();
    if (currentRow < 100) {
      try {
        console.log("Processing row:", row);
        await insertData(row);
      } catch (err) {
        console.error("Error processing row:", err);
      } finally {
        stream.resume(); 
      }
    }
  });

  stream.on("end", async () => {
    try {
      res.status(200).send("CSV file processed and data stored in PostgreSQL");
    } catch (err) {
      console.error("Error sending response:", err);
      res.status(500).send("Error sending response");
    } finally {
      await pool.end(); 
    }
  });

  stream.on("error", (error) => {
    console.error("Error reading CSV file:", error);
    res.status(500).send("Error processing CSV file");
  });
};

// Function to insert a batch of data
// const insertBatchData = async (batch) => {
//     if (batch.length === 0) return;
  
//     try {
//       const columns = Object.keys(batch[0]);
  
//       // Constructing bulk INSERT query
//       const valuePlaceholders = batch
//         .map((_, i) =>
//           `(${columns.map((_, j) => `$${i * columns.length + j + 1}`).join(", ")})`
//         )
//         .join(", ");
  
//       const insertQuery = `INSERT INTO books (${columns.join(", ")}) VALUES ${valuePlaceholders}`;
  
//       // Flattening the values into a single array
//       const values = batch.flatMap(Object.values);
  
//       await pool.query(insertQuery, values);
//       console.log(`${batch.length} rows inserted successfully`);
//     } catch (err) {
//       console.error("Error inserting batch data:", err);
//     }
//   };

// // CSV upload function with batch processing
// exports.uploadData = (req, res) => {
//     const filePath = "./utils/Sample book data.csv"; // Path to the CSV file
//     const batchSize = 100; // Number of rows per batch
//     let batch = [];
//     let currentRow = 0;
  
//     pool.connect();
  
//     const stream = fs.createReadStream(filePath).pipe(csv());
  
//     stream.on("headers", async (headers) => {
//       await createTable(headers);
//     });
  
//     stream.on("data", async (row) => {
//       currentRow++;
//       batch.push(row);
  
//       if (batch.length >= batchSize) {
//         stream.pause(); // Pause stream to handle batch insertion
  
//         try {
//           console.log(`Processing batch of ${batch.length} rows...`);
//           await insertBatchData(batch); // Insert batch
//           batch = []; // Clear batch after insertion
//         } catch (err) {
//           console.error("Error processing batch:", err);
//         } finally {
//           stream.resume(); // Resume stream after inserting the batch
//         }
//       }
//     });
  
//     stream.on("end", async () => {
//       // Insert any remaining rows that didn't form a complete batch
//       if (batch.length > 0) {
//         await insertBatchData(batch);
//       }
  
//       try {
//         res.status(200).send("CSV file processed and data stored in PostgreSQL");
//       } catch (err) {
//         console.error("Error sending response:", err);
//         res.status(500).send("Error sending response");
//       } finally {
//         await pool.end(); // Close the pool when done
//       }
//     });
  
//     stream.on("error", (error) => {
//       console.error("Error reading CSV file:", error);
//       res.status(500).send("Error processing CSV file");
//     });
//   };

// Validation function
const validateData = (data) => {
    const errors = [];
  
    try {
      if (!data.title || data.title.trim() === "") {
        errors.push("Title is required.");
      }
  
      if (
        data.average_rating &&
        (data.average_rating < 0 || data.average_rating > 5)
      ) {
        errors.push("Average rating must be between 0 and 5.");
      }
  
      if (data.isbn && data.isbn.length > 15) {
        errors.push("ISBN must be at most 15 characters.");
      }
  
      if (data.isbn13 && data.isbn13.length > 20) {
        errors.push("ISBN13 must be at most 20 characters.");
      }
  
      if (data.language_code && data.language_code.length > 10) {
        errors.push("Language code must be at most 10 characters.");
      }
    } catch (e) {
      errors.push("Unknown error while inserting the bookId : " + data.bookId);
    }
  
    return errors;
  };
  