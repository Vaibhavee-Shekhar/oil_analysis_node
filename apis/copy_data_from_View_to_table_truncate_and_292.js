const express = require('express');
const fs = require('fs');
const { connectToDatabase } = require('./connect3.js'); // Database connection module

const router = express.Router();

// Utility to log data to JSON file
const logDataToFile = (logFile, data) => {
    const currentDate = new Date().toISOString().split('T')[0]; // Get current date
    const logEntry = {
        date: currentDate,
        deleted_records: data
    };

    if (fs.existsSync(logFile)) {
        const existingData = JSON.parse(fs.readFileSync(logFile, 'utf-8'));
        existingData.push(logEntry);
        fs.writeFileSync(logFile, JSON.stringify(existingData, null, 2), 'utf-8');
    } else {
        fs.writeFileSync(logFile, JSON.stringify([logEntry], null, 2), 'utf-8');
    }
};

// Main function to process the data
const processData = async () => {
    const logFile = './292_654_deleted.json';
    const truncateQueries = [
        "DELETE FROM dispute;",
        "DELETE FROM dispute_all_orders;",
        "DELETE FROM fc_oil_change;",
        // Add remaining truncate queries here...
        "DELETE FROM [NewDatabase].[dbo].[dispute_state_wise_count];"
    ];

    const deleteQuery = "DELETE FROM [dbo].[consumption_analysis_table]";
    const fetchQuery = `
        SELECT [MATERIAL], [PLANT], [MOVE_TYPE], [VAL_TYPE], [POSTING_DATE], 
               [ENTRY_DATE], [QUANTITY], [UNIT], [FUNCTIONAL_LOCATION], 
               [SERVICE_ORDER_NUMBER], [STOR_LOC], [DOCUMENT_NUMBER], 
               [ZZAUFNR], [ZTEXT1], [TXTMD], [order_type], 
               [current_Oil_change_date]
        FROM [dbo].[vw_consumption_analysis]`;

    const insertQuery = `
        INSERT INTO [dbo].[consumption_analysis_table]
        ([MATERIAL], [PLANT], [MOVE_TYPE], [VAL_TYPE], [POSTING_DATE], 
         [ENTRY_DATE], [QUANTITY], [UNIT], [FUNCTIONAL_LOCATION], 
         [SERVICE_ORDER_NUMBER], [STOR_LOC], [DOCUMENT_NUMBER], 
         [ZZAUFNR], [ZTEXT1], [TXTMD], [order_type], 
         [current_oil_change_date])
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    const logQueries = [
        // Query 1: Rows with MOVE_TYPE = 292
        `
        SELECT *
        FROM [NewDatabase].[dbo].[consumption_analysis_table]
        WHERE [SERVICE_ORDER_NUMBER] IN (
            SELECT [SERVICE_ORDER_NUMBER]
            FROM [NewDatabase].[dbo].[consumption_analysis_table]
            GROUP BY [SERVICE_ORDER_NUMBER]
            HAVING SUM(CASE WHEN [MOVE_TYPE] = '292' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN [MOVE_TYPE] <> '292' THEN 1 ELSE 0 END) = 0
        )`,
        // Query 2: Rows with MOVE_TYPE = 654
        `
        SELECT *
        FROM [NewDatabase].[dbo].[consumption_analysis_table]
        WHERE [SERVICE_ORDER_NUMBER] IN (
            SELECT [SERVICE_ORDER_NUMBER]
            FROM [NewDatabase].[dbo].[consumption_analysis_table]
            GROUP BY [SERVICE_ORDER_NUMBER]
            HAVING SUM(CASE WHEN [MOVE_TYPE] = '654' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN [MOVE_TYPE] <> '654' THEN 1 ELSE 0 END) = 0
        )`
    ];

    try {
        const db = await connectToDatabase();

        // Truncate tables
        for (const query of truncateQueries) {
            await db.query(query);
        }

        // Clear the target table
        await db.query(deleteQuery);

        // Fetch data in chunks of 2000 and insert into target table
        let offset = 0;
        let rows;

        do {
            rows = await db.query(`${fetchQuery} OFFSET ${offset} ROWS FETCH NEXT 2000 ROWS ONLY`);
            for (const row of rows) {
                await db.query(insertQuery, [
                    row.MATERIAL, row.PLANT, row.MOVE_TYPE, row.VAL_TYPE,
                    row.POSTING_DATE, row.ENTRY_DATE, row.QUANTITY, row.UNIT,
                    row.FUNCTIONAL_LOCATION, row.SERVICE_ORDER_NUMBER,
                    row.STOR_LOC, row.DOCUMENT_NUMBER, row.ZZAUFNR,
                    row.ZTEXT1, row.TXTMD, row.order_type,
                    row.current_Oil_change_date
                ]);
            }
            offset += rows.length;
        } while (rows.length === 2000);

        // Fetch and log rows based on conditions
        let combinedResults = [];
        for (const query of logQueries) {
            const results = await db.query(query);
            combinedResults = combinedResults.concat(results);
        }

        logDataToFile(logFile, combinedResults);

        // Delete rows one by one based on SERVICE_ORDER_NUMBER
        const serviceOrderNumbers = [...new Set(combinedResults.map(row => row.SERVICE_ORDER_NUMBER))];
        for (const serviceOrderNumber of serviceOrderNumbers) {
            await db.query(`
                DELETE FROM [NewDatabase].[dbo].[consumption_analysis_table]
                WHERE [SERVICE_ORDER_NUMBER] = ?`, [serviceOrderNumber]);
        }

        console.log("Data refreshed, rows logged, and deleted successfully.");
    } catch (error) {
        console.error("Error processing data:", error);
    }
};

processData();

module.exports = router;
