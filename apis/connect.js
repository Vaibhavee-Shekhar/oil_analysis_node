const odbc = require('odbc');

// ODBC connection string
const connectionString = 'Driver={ODBC Driver 17 for SQL Server};Server=SELPUNPWRBI02,1433;Database=Meet;Uid=LubricationPortal;Pwd=Kitkat998;Encrypt=no;TrustServerCertificate=yes;Connection Timeout=30;';

// Function to connect to the database using ODBC
async function connectToDatabase() {
    try {
        // Establish a connection using ODBC
        const connection = await odbc.connect(connectionString);
        console.log('Connected to the database.');

        // Optional: You can execute a simple query to test the connection
        const result = await connection.query('SELECT 1 AS test');
        console.log('Test query result:', result);

        return connection; // Return the connection for further use
    } catch (err) {
        console.error('Database connection failed:', err);
        throw err; // Re-throw the error for handling upstream
    }
}

// Example usage of the connectToDatabase function
(async () => {
    try {
        const dbConnection = await connectToDatabase();
        // Use the dbConnection as needed
        // Don't forget to close the connection when done
        await dbConnection.close();
    } catch (error) {
        console.error('Error:', error);
    }
})();

// Export only the connectToDatabase function
module.exports = { connectToDatabase };
