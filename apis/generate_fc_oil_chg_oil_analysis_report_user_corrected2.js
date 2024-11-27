const express = require('express');
const { connectToDatabase } = require('./connect3.js'); // Your ODBC connection module
const router = express.Router();

router.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    next();
});

router.get('/process-data', async (req, res) => {
    try {
        const db = await connectToDatabase();
        console.log('Database connected.');

        // Query to fetch data in chunks
        const chunkSize = 2000;
        let offset = 0;
        let results = [];
        const validMaterials = ['51033078', '51055772', '51107303'];

        while (true) {
            const query = `
                WITH ServiceOrders AS (
                    SELECT *,
                        CASE 
                            WHEN [MATERIAL] IN (${validMaterials.map(() => '?').join(',')})
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
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;`;

            const chunk = await db.query(query, [...validMaterials, offset, chunkSize]);
            if (chunk.length === 0) break;
            results = results.concat(chunk);
            offset += chunkSize;
        }

        // Process the data
        const processedData = processResults(results, validMaterials);

        // Insert processed data into the database
        for (const record of processedData) {
            await insertData(db, record, 'gb_oil_change');
        }

        res.status(200).json({ message: 'Data processed successfully.' });
    } catch (error) {
        console.error('Error processing data:', error);
        res.status(500).json({ message: 'An error occurred.', error });
    }
});

// Function to process data
function processResults(results, validMaterials) {
    const finalData = {};
    const orderResults = [];

    results.forEach(row => {
        const serviceOrderNumber = row['SERVICE_ORDER_NUMBER'];
        const moveType = row['MOVE_TYPE'];
        const quantity = parseFloat(row['QUANTITY']);
        const material = row['MATERIAL'];

        if (validMaterials.includes(material)) {
            if (!finalData[serviceOrderNumber]) {
                finalData[serviceOrderNumber] = {
                    issue: 0,
                    return: 0,
                    functional_location: row['FUNCTIONAL_LOCATION'],
                    material: row['MATERIAL'],
                    plant: row['PLANT'],
                    move_type: row['MOVE_TYPE'],
                    val_type: row['VAL_TYPE'],
                    posting_date: row['POSTING_DATE'],
                    entry_date: row['ENTRY_DATE'],
                    ztext1: row['ZTEXT1'],
                    current_Oil_change_date: row['current_Oil_change_date'] || null,
                    order_type: row['order_type'],
                    material_description: row['TXTMD'],
                    STOR_LOC: row['STOR_LOC'],
                    DOCUMENT_NUMBER: row['DOCUMENT_NUMBER'],
                };
            }

            if (['291', '292'].includes(moveType)) {
                finalData[serviceOrderNumber].issue += quantity;
            } else if (['653', '654'].includes(moveType)) {
                finalData[serviceOrderNumber].return += quantity;
            }
        }
    });

    Object.entries(finalData).forEach(([serviceOrderNumber, data]) => {
        const issue = data.issue;
        const ret = data.return;
        const percentageIssueReturn = issue !== 0 ? Math.abs((ret / issue) * 100) : 0;
        const totalQuantity = Math.abs(issue + ret);

        // Process classification logic...
        const record = {
            SERVICE_ORDER_NUMBER: serviceOrderNumber,
            FUNCTIONAL_LOCATION: data.functional_location,
            ISSUE: issue,
            RETURN: ret,
            PERCENTAGE_ISSUE_RETURN: percentageIssueReturn,
            MATERIAL: data.material,
            ...data,
        };

        orderResults.push(record);
    });

    return orderResults;
}

// Function to insert data dynamically
async function insertData(db, record, tableName) {
    const columnMap = {
        'SERVICE_ORDER_NUMBER': '[Order No]',
        'FUNCTIONAL_LOCATION': '[Function Loc]',
        'ISSUE': '[Issue]',
        'RETURN': '[Return]',
        'PERCENTAGE_ISSUE_RETURN': '[Return Percentage]',
        'PLANT': '[Plant]',
        'MATERIAL': '[Material]',
        'STOR_LOC': '[Storage Location]',
        'MOVE_TYPE': '[Move Type]',
        'DOCUMENT_NUMBER': '[Material Document]',
        'VAL_TYPE': '[Val Type]',
        'POSTING_DATE': '[Posting Date]',
        'ENTRY_DATE': '[Entry Date]',
    };

    const columns = [];
    const values = [];
    const placeholders = [];

    Object.entries(columnMap).forEach(([key, column]) => {
        if (record[key] !== undefined) {
            columns.push(column);
            values.push(record[key]);
            placeholders.push('?');
        }
    });

    const query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`;
    await db.query(query, values);
}

module.exports = router;
