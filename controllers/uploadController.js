const fs = require("fs");
const csv = require("csv-parser");
const { Pool } = require("pg");

const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "postgres",
  password: "chandru",
  port: 5432,
});

const isValidDateMMDDYYYY = (dateString) => {
  // Regular expression to match MM-DD-YYYY format
  const regex = /^(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])-(\d{4})$/;

  // Check if dateString matches the regex
  if (!regex.test(dateString)) return false;

  // Split the dateString into components
  const parts = dateString.split("-");
  const month = parseInt(parts[0], 10);
  const day = parseInt(parts[1], 10);
  const year = parseInt(parts[2], 10);

  // Create a new Date object
  const date = new Date(year, month - 1, day); // month is 0-indexed

  // Check if the date is valid and matches the original input
  return (
    date.getFullYear() === year &&
    date.getMonth() === month - 1 &&
    date.getDate() === day
  );
};

const createTable = async () => {
  try {
    const dropTableQuery = `DROP TABLE IF EXISTS books;`;
    await pool.query(dropTableQuery);

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS books (
        bookID SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        authors TEXT,
        average_rating NUMERIC(3, 2),
        isbn VARCHAR(15),
        isbn13 VARCHAR(20),
        language_code VARCHAR(10),
        num_pages INT,
        ratings_count INT,
        text_reviews_count INT,
        publication_date TEXT,
        publisher VARCHAR(255)
      );
    `;
    await pool.query(createTableQuery);
    console.log("Table created successfully");
  } catch (err) {
    console.error("Error creating table:", err);
    throw err; // Propagate the error to be handled in uploadData
  }
};

exports.uploadData = async (req, res) => {
  const filePath = req.file ? req.file.path : "./utils/Sample book data.csv";
  const batchSize = 100;
  let batch = [];

  try {
    // Create the table before processing the CSV
    await createTable();

    const stream = fs.createReadStream(filePath).pipe(csv());

    stream.on("data", (row) => {
      batch.push(row);

      if (batch.length >= batchSize) {
        stream.pause();
        processBatch(batch)
          .then(() => {
            batch = [];
            stream.resume();
          })
          .catch((err) => {
            console.error("Error processing batch:", err);
            res.status(500).send("Error processing batch");
          });
      }
    });

    stream.on("end", async () => {
      try {
        if (batch.length > 0) {
          await insertBatchData(batch);
        }
        res
          .status(200)
          .send("CSV file processed and data stored in PostgreSQL");
      } catch (err) {
        console.error("Error processing final batch:", err);
        res.status(500).send("Error processing CSV file");
      } finally {
        await pool.end();
      }
    });

    stream.on("error", (error) => {
      console.error("Error reading CSV file:", error);
      res.status(500).send("Error processing CSV file");
      pool.end();
    });
  } catch (err) {
    console.error("Error in uploadData:", err);
    res.status(500).send("Error creating table");
  }
};

const insertBatchData = async (batch) => {
  if (batch.length === 0) return;

  const validRows = [];
  const invalidRows = [];

  for (const row of batch) {
    const errors = validateData(row);
    if (errors.length === 0) {
      validRows.push(row);
    } else {
      invalidRows.push({ row, errors });
    }
  }

  if (validRows.length === 0) {
    console.log("No valid rows to insert in this batch.");
    return { inserted: 0, invalid: invalidRows };
  }

  try {
    const columns = Object.keys(validRows[0]);

    // Constructing bulk INSERT query
    const valuePlaceholders = validRows
      .map(
        (_, i) =>
          `(${columns
            .map((_, j) => `$${i * columns.length + j + 1}`)
            .join(", ")})`
      )
      .join(", ");

    const insertQuery = `INSERT INTO books (${columns.join(
      ", "
    )}) VALUES ${valuePlaceholders}`;

    // Flattening the values into a single array
    const values = validRows.flatMap(Object.values);

    await pool.query(insertQuery, values);
    console.log(`${validRows.length} rows inserted successfully`);
    return { inserted: validRows.length, invalid: invalidRows };
  } catch (err) {
    console.error("Error inserting batch data:", err);
    return {
      inserted: 0,
      invalid: [
        ...invalidRows,
        ...validRows.map((row) => ({
          row,
          errors: ["Database insertion error"],
        })),
      ],
    };
  }
};

async function processBatch(batch) {
  try {
    await insertBatchData(batch);
  } catch (err) {
    console.error("Error processing batch:", err);
  }
}

const validateData = (data) => {
  const errors = [];
  if (!data.title || data.title.trim() === "") {
    errors.push("Title is required.");
  }

  // Usage in your validateData function
  if (data.publication_date && !isValidDateMMDDYYYY(data.publication_date)) {
    errors.push("Publication date must be in MM-DD-YYYY format.");
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
  return errors;
};
