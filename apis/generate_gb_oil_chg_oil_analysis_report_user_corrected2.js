const express = require('express');
const odbc = require('odbc');
const fs = require('fs'); // For optional logging or saving data
const router = express.Router();

// ODBC connection string
const connectionString = 'Driver={ODBC Driver 17 for SQL Server};Server=SELPUNPWRBI02,1433;Database=Meet;Uid=LubricationPortal;Pwd=Kitkat998;Encrypt=no;TrustServerCertificate=yes;Connection Timeout=30;';
const BATCH_SIZE = 2000; // Number of rows to process per batch

// Connect to the database
async function connectToDatabase() {
    try {
        const connection = await odbc.connect(connectionString);
        console.log('Connected to the database.');
        return connection;
    } catch (err) {
        console.error('Database connection failed:', err.message);
        throw err;
    }
}

// Fetch data in chunks
async function fetchDataInChunks(connection, offset, batchSize) {
    const query = `
        WITH ServiceOrders AS (
            SELECT *,
                CASE 
                    WHEN MATERIAL IN ('51028446', '51028444', '51028447', '51028445', '51028449',
                                      '51079898', '51077849', '51077531', '51028448', '51063867')
                    THEN 1 ELSE 0 END AS material_in_list
            FROM [NewDatabase].[dbo].[consumption_analysis_table]
            WHERE NOT (order_type LIKE 'yd%' OR order_type LIKE 'pd%')
                  OR (order_type IS NULL OR order_type = '')
        )
        SELECT *
        FROM ServiceOrders
        WHERE service_order_number IN (
            SELECT service_order_number
            FROM ServiceOrders
            GROUP BY service_order_number
            HAVING SUM(material_in_list) > 0
        )
        ORDER BY service_order_number
        OFFSET ${offset} ROWS FETCH NEXT ${batchSize} ROWS ONLY;
    `;
    return connection.query(query);
}

// Process the data
function processResults(results) {
    const validMaterials = [
        '51028446', '51028444', '51028447', '51028445', '51028449',
        '51079898', '51077849', '51077531', '51028448', '51063867'
    ];

    const finalData = {};
    const orderResults = [];

    results.forEach(row => {
        const serviceOrderNumber = row.SERVICE_ORDER_NUMBER;
        const moveType = row.MOVE_TYPE;
        const quantity = parseFloat(row.QUANTITY || 0);
        const material = row.MATERIAL;

        if (validMaterials.includes(material)) {
            if (!finalData[serviceOrderNumber]) {
                finalData[serviceOrderNumber] = {
                    issue: 0,
                    return: 0,
                    functional_location: row.FUNCTIONAL_LOCATION,
                    material: row.MATERIAL,
                    plant: row.PLANT,
                    move_type: row.MOVE_TYPE,
                    val_type: row.VAL_TYPE,
                    posting_date: row.POSTING_DATE,
                    entry_date: row.ENTRY_DATE,
                    ztext1: row.ZTEXT1,
                    current_Oil_change_date: row.current_Oil_change_date,
                    order_type: row.order_type,
                    material_description: row.TXTMD,
                    STOR_LOC: row.STOR_LOC,
                    DOCUMENT_NUMBER: row.DOCUMENT_NUMBER,
                };
            }

            if (['291', '292'].includes(moveType)) {
                finalData[serviceOrderNumber].issue += quantity;
            } else if (['653', '654'].includes(moveType)) {
                finalData[serviceOrderNumber].return += quantity;
            }
        }
    });

    // Process classification logic and prepare results
    for (const serviceOrderNumber in finalData) {
        const data = finalData[serviceOrderNumber];
        const issue = data.issue;
        const returnQty = data.return;
        const percentageIssueReturn = issue !== 0 ? Math.abs(returnQty / issue) * 100 : 0;

        // Example classification (based on PHP logic)
        if (percentageIssueReturn > 80 && percentageIssueReturn < 100) {
            const classification = 'GB_OIL_CHANGE ORDER';
            orderResults.push({
                SERVICE_ORDER_NUMBER: serviceOrderNumber,
                FUNCTIONAL_LOCATION: data.functional_location,
                QUANTITY: Math.abs(issue + returnQty),
                ISSUE: issue,
                RETURN: returnQty,
                PERCENTAGE_ISSUE_RETURN: percentageIssueReturn,
                MATERIAL: data.material,
                ORDER_TYPE: data.order_type,
                PLANT: data.plant,
                MOVE_TYPE: data.move_type,
                POSTING_DATE: data.posting_date,
                ENTRY_DATE: data.entry_date,
                ZTEXT1: data.ztext1,
                MATERIAL_DESCRIPTION: data.material_description,
                Component: 'GB', // Example
                Classification: classification
            });
        }
    }

    return orderResults;
}

// Insert data into the database
async function insertData(connection, orderResults, tableName) {
    for (const record of orderResults) {
        const columns = Object.keys(record).map(key => `[${key}]`).join(', ');
        const values = Object.values(record).map(value => `'${value}'`).join(', ');

        const query = `INSERT INTO ${tableName} (${columns}) VALUES (${values})`;
        await connection.query(query);
    }
}

// Main function
async function main() {
    try {
        const connection = await connectToDatabase();
        let offset = 0;
        let results;

        do {
            // Fetch data in chunks
            results = await fetchDataInChunks(connection, offset, BATCH_SIZE);
            if (results.length === 0) break;

            // Process results
            const orderResults = processResults(results);

            // Insert results into the database
            if (orderResults.length > 0) {
                await insertData(connection, orderResults, '[gb_oil_change]');
                console.log(`Processed and inserted ${orderResults.length} records.`);
            }

            offset += BATCH_SIZE; // Increment offset for the next batch
        } while (results.length === BATCH_SIZE);

        console.log('Data processing complete.');
        await connection.close();
    } catch (error) {
        console.error('Error:', error.message);
    }
}

// Run the script
main();

module.exports = router;
