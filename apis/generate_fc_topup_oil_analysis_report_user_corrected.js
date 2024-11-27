const express = require('express');
const { connectToDatabase } = require('./connect3.js');
const fs = require('fs');
const router = express.Router();

// Utility to convert date to SQL format (YYYY-MM-DD)
const convertDateToSQLFormat = (date) => {
    const [day, month, year] = date.split('-');
    return `${year}-${month}-${day}`;
};

// Helper function to insert data into the database
const insertData = async (connection, result, tableName) => {
    const columnMap = {
        SERVICE_ORDER_NUMBER: '[Order No]',
        FUNCTIONAL_LOCATION: '[Function Loc]',
        ISSUE: '[Issue]',
        RETURN: '[Return]',
        PERCENTAGE_ISSUE_RETURN: '[Return Percentage]',
        PLANT: '[Plant]',
        STATE: '[State]',
        AREA: '[Area]',
        SITE: '[Site]',
        MATERIAL: '[Material]',
        STOR_LOC: '[Storage Location]',
        MOVE_TYPE: '[Move Type]',
        DOCUMENT_NUMBER: '[Material Document]',
        MATERIAL_DESCRIPTION: '[Description]',
        VAL_TYPE: '[Val Type]',
        POSTING_DATE: '[Posting Date]',
        ENTRY_DATE: '[Entry Date]',
        QUANTITY: '[Quantity]',
        ORDER_TYPE: '[Order Type]',
        WTG_Model: '[WTG Model]',
        current_Oil_change_date: '[Current Oil Change Date]',
        ZTEXT1: '[Order Status]',
        date_of_insertion: '[date_of_insertion]',
    };

    const columns = [];
    const values = [];
    const placeholders = [];

    for (const key in columnMap) {
        if (result[key] !== undefined) {
            columns.push(columnMap[key]);
            placeholders.push('?');
            if (['POSTING_DATE', 'ENTRY_DATE', 'current_Oil_change_date'].includes(key)) {
                values.push(convertDateToSQLFormat(result[key]));
            } else {
                values.push(result[key]);
            }
        }
    }

    const query = `
        INSERT INTO ${tableName} (${columns.join(', ')}) 
        VALUES (${placeholders.join(', ')})
    `;

    await connection.query(query, values);
};

const processData = async (connection) => {
    const validMaterials = ['51033078', '51055772', '51107303'];

    const query = `
        WITH ServiceOrders AS (
            SELECT *, CASE 
                WHEN MATERIAL IN ('51033078', '51055772', '51107303') THEN 1 ELSE 0 END AS material_in_list
            FROM [NewDatabase].[dbo].[consumption_analysis_table]
            WHERE NOT (order_type LIKE 'yd%' OR order_type LIKE 'pd%' OR order_type LIKE 'GB%' OR order_type LIKE 'FC%')
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
    `;

    const results = await connection.query(query);

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
    }

    for (const serviceOrderNumber in finalData) {
        const data = finalData[serviceOrderNumber];
        const percentageIssueReturn = data.issue !== 0 ? Math.abs(data.return / data.issue) * 100 : 0;
        const absIssue = Math.abs(data.issue);

        const thresholdQuery = `
            SELECT TOP 1 [ten_percent_reduced_value]
            FROM [NewDatabase].[dbo].[fc_threshold]
            WHERE material_code = ? AND wtg_model = ?
            ORDER BY [ten_percent_reduced_value] ASC
        `;

        const thresholdResult = await connection.query(thresholdQuery, [data.material, data.WTG_Model]);

        if (thresholdResult.length > 0) {
            const tenPercentReducedValue = parseFloat(thresholdResult[0].ten_percent_reduced_value);

            if ((percentageIssueReturn > 80 || percentageIssueReturn === 0) && absIssue < tenPercentReducedValue) {
                orderResults.push({
                    ...data,
                    SERVICE_ORDER_NUMBER: serviceOrderNumber,
                    PERCENTAGE_ISSUE_RETURN: percentageIssueReturn,
                    ten_Percent_reduced_value: tenPercentReducedValue,
                });

                await insertData(connection, data, 'fc_topup');
            }
        }
    }
};

router.post('/process', async (req, res) => {
    try {
        const connection = await connectToDatabase();
        await processData(connection);
        res.send('Data processing completed.');
    } catch (error) {
        console.error(error);
        res.status(500).send('Error processing data.');
    }
});

module.exports = router;
