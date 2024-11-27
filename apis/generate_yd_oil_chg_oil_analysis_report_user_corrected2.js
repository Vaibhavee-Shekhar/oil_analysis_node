const express = require('express');
const odbc = require('odbc');
const fs = require('fs');
const path = require('path');

// Database connection
const connectionString = 'Driver={ODBC Driver 17 for SQL Server};Server=SELPUNPWRBI02,1433;Database=Meet;Uid=LubricationPortal;Pwd=Kitkat998;Encrypt=no;TrustServerCertificate=yes;Connection Timeout=30;';

async function connectToDatabase() {
    try {
        const connection = await odbc.connect(connectionString);
        console.log('Connected to the database.');
        return connection;
    } catch (err) {
        console.error('Database connection failed:', err);
        throw err;
    }
}

async function fetchDataInChunks(query, connection, chunkSize = 2000) {
    try {
        let offset = 0;
        const results = [];

        while (true) {
            const paginatedQuery = `${query} OFFSET ${offset} ROWS FETCH NEXT ${chunkSize} ROWS ONLY`;
            const chunk = await connection.query(paginatedQuery);
            if (chunk.length === 0) break;
            results.push(...chunk);
            offset += chunkSize;
        }

        return results;
    } catch (err) {
        console.error('Error fetching data in chunks:', err);
        throw err;
    }
}

async function processData(results) {
    const validMaterials = ['51028446'];
    const finalData = {};
    const orderResults = [];

    for (const row of results) {
        const serviceOrderNumber = row.SERVICE_ORDER_NUMBER;
        const moveType = row.MOVE_TYPE;
        const quantity = parseFloat(row.QUANTITY);
        const material = row.MATERIAL;

        if (validMaterials.includes(material)) {
            if (!finalData[serviceOrderNumber]) {
                finalData[serviceOrderNumber] = {
                    issue: 0,
                    return: 0,
                    functional_location: row.FUNCTIONAL_LOCATION,
                    material,
                    plant: row.PLANT,
                    move_type: moveType,
                    val_type: row.VAL_TYPE,
                    posting_date: row.POSTING_DATE,
                    entry_date: row.ENTRY_DATE,
                    ztext1: row.ZTEXT1,
                    current_Oil_change_date: row.current_Oil_change_date || null,
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
    }

    for (const serviceOrderNumber in finalData) {
        const data = finalData[serviceOrderNumber];
        const issue = data.issue;
        const returnQuantity = data.return;
        const absIssue = Math.abs(issue);

        const percentageIssueReturn = issue !== 0 ? Math.abs((returnQuantity / issue) * 100) : 0;

        if (percentageIssueReturn > 80 && percentageIssueReturn < 100) {
            orderResults.push({
                SERVICE_ORDER_NUMBER: serviceOrderNumber,
                FUNCTIONAL_LOCATION: data.functional_location,
                QUANTITY: Math.abs(issue + returnQuantity),
                ISSUE: issue,
                RETURN: returnQuantity,
                PERCENTAGE_ISSUE_RETURN: percentageIssueReturn,
                STATE: data.state,
                AREA: data.area,
                SITE: data.site,
                WTG_Model: data.WTG_Model,
                STOR_LOC: data.STOR_LOC,
                DOCUMENT_NUMBER: data.DOCUMENT_NUMBER,
                MATERIAL: data.material,
                MATERIAL_DESCRIPTION: data.material_description,
                PLANT: data.plant,
                MOVE_TYPE: data.move_type,
                VAL_TYPE: data.val_type,
                POSTING_DATE: data.posting_date,
                ENTRY_DATE: data.entry_date,
                ZTEXT1: data.ztext1,
                current_Oil_change_date: data.current_Oil_change_date,
                ORDER_TYPE: data.order_type,
                Order: 'YD',
                Component: 'YD',
            });
        }
    }

    return orderResults;
}

async function saveResultsToFile(results, filePath) {
    try {
        const jsonData = JSON.stringify(results, null, 2);
        fs.writeFileSync(filePath, jsonData, 'utf8');
        console.log('Data saved to file:', filePath);
    } catch (err) {
        console.error('Error saving results to file:', err);
    }
}

async function main() {
    const connection = await connectToDatabase();
    const query = `
        WITH ServiceOrders AS (
            SELECT *,
                CASE WHEN [MATERIAL] IN ('51028446') THEN 1 ELSE 0 END AS material_in_list
            FROM [NewDatabase].[dbo].[consumption_analysis_table]
            WHERE NOT (order_type LIKE 'GB%' OR order_type LIKE 'FC%' OR order_type LIKE 'pd%')
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
        ORDER BY service_order_number;
    `;

    try {
        const results = await fetchDataInChunks(query, connection);
        const processedResults = await processData(results);
        const outputFilePath = path.join(__dirname, 'results-yd_oil_chg_check.json');
        await saveResultsToFile(processedResults, outputFilePath);
    } catch (err) {
        console.error('Error in main execution:', err);
    } finally {
        await connection.close();
    }
}

main();
