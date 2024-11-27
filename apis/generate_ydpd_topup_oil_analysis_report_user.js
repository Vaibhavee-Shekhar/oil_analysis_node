const express = require('express');
const odbc = require('odbc');
const { connectToDatabase } = require('./connect3.js'); // Your database connection module
const ExcelJS = require('exceljs'); // Assuming you might need it for file generation

const router = express.Router();

// Set up CORS middleware
router.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.header('Access-Control-Allow-Credentials', 'true');
    res.header('Access-Control-Max-Age', '3600');
    next();
});

// SQL Query to fetch data (converted from PHP)
const query = `
WITH ServiceOrders AS (
    SELECT *,
           CASE 
               WHEN [MATERIAL] IN ('51028446')
               THEN 1 ELSE 0 END AS material_in_list
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
ORDER BY service_order_number;
`;

const validMaterials = ['51028446']; // Materials to check

// Function to process and insert data
async function processAndInsertData(db) {
    try {
        const results = await fetchDataInBatches(db);

        const finalData = {};
        const orderResults = [];

        // Process the results in batches
        results.forEach(row => {
            const serviceOrderNumber = row.SERVICE_ORDER_NUMBER;
            const moveType = row.MOVE_TYPE;
            const quantity = parseFloat(row.QUANTITY);
            const material = row.MATERIAL;

            if (validMaterials.includes(material)) {
                // Initialize if not already initialized
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
                        current_Oil_change_date: row.current_Oil_change_date || null,
                        order_type: row.order_type,
                        material_description: row.TXTMD,
                        stor_loc: row.STOR_LOC,
                        document_number: row.DOCUMENT_NUMBER,
                    };
                }

                // Accumulate issue or return based on move type
                if (moveType === '291' || moveType === '292') {
                    finalData[serviceOrderNumber].issue += quantity;
                } else if (moveType === '653' || moveType === '654') {
                    finalData[serviceOrderNumber].return += quantity;
                }
            }
        });

        // Further processing for final data
        for (const serviceOrderNumber in finalData) {
            const data = finalData[serviceOrderNumber];
            const { issue, return: returnQty, material, functional_location } = data;
            const percentageIssueReturn = issue !== 0 ? (Math.abs(returnQty) / Math.abs(issue)) * 100 : 0;

            // Fetch WTG_Model details
            const wtgModelDetails = await fetchWTGModel(db, functional_location);
            if (wtgModelDetails) {
                const { wtgModel, state, area, site } = wtgModelDetails;

                // Fetch oil model details
                const oilDetails = await fetchOilDetails(db, wtgModel);
                if (oilDetails) {
                    const { totalYawDriveOil, totalPitchDriveOil } = oilDetails;
                    const absIssue = Math.abs(issue);

                    // Insert data only if conditions match
                    if ((percentageIssueReturn > 80 && percentageIssueReturn < 100) || percentageIssueReturn === 0) {
                        if (absIssue < totalYawDriveOil && absIssue < totalPitchDriveOil) {
                            const classification = 'ydpd top up';

                            orderResults.push({
                                SERVICE_ORDER_NUMBER: serviceOrderNumber,
                                FUNCTIONAL_LOCATION: functional_location,
                                QUANTITY: Math.abs(issue + returnQty),
                                TOTAL_QUANTITY: Math.abs(issue + returnQty),
                                ISSUE: issue,
                                RETURN: returnQty,
                                PERCENTAGE_ISSUE_RETURN: percentageIssueReturn,
                                CLASSIFICATION: classification,
                                STATE: state,
                                AREA: area,
                                SITE: site,
                                WTG_Model: wtgModel,
                                STOR_LOC: data.stor_loc,
                                DOCUMENT_NUMBER: data.document_number,
                                total_yaw_drive_oil_per_wtg: totalYawDriveOil,
                                total_pitch_drive_oil_per_wtg: totalPitchDriveOil,
                                MATERIAL: material,
                                MATERIAL_DESCRIPTION: data.material_description,
                                PLANT: data.plant,
                                MOVE_TYPE: data.move_type,
                                VAL_TYPE: data.val_type,
                                POSTING_DATE: data.posting_date,
                                ENTRY_DATE: data.entry_date,
                                ZTEXT1: data.ztext1,
                                current_Oil_change_date: data.current_Oil_change_date,
                                ORDER_TYPE: data.order_type,
                            });

                            // Insert data into the table
                            await insertData(db, orderResults[orderResults.length - 1]);
                        }
                    }
                }
            }
        }

        console.log('Processing complete.');
    } catch (err) {
        console.error('Error processing data:', err);
    }
}

// Function to fetch data in batches of 2000
async function fetchDataInBatches(db) {
    let results = [];
    let offset = 0;
    const limit = 2000;
    let moreData = true;

    while (moreData) {
        const batchQuery = `${query} OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
        const batchResults = await db.query(batchQuery);
        if (batchResults.length > 0) {
            results = results.concat(batchResults);
            offset += limit;
        } else {
            moreData = false;
        }
    }
    return results;
}

// Function to fetch WTG Model details
async function fetchWTGModel(db, functionalLocation) {
    const query = `SELECT [WTG_Model], [State], [Area], [Site] 
                   FROM [NewDatabase].[dbo].[installedbase] 
                   WHERE [Functional_Location] = ?`;

    const result = await db.query(query, [functionalLocation]);
    return result.length > 0 ? result[0] : null;
}

// Function to fetch oil model details
async function fetchOilDetails(db, wtgModel) {
    const query = `SELECT TOP 1 [total_yaw_drive_oil_per_wtg], [total_pitch_drive_oil_per_wtg] 
                   FROM [NewDatabase].[dbo].[oil_model_master] 
                   WHERE [wtg_model] = ? 
                   ORDER BY [total_yaw_drive_oil_per_wtg] ASC, [total_pitch_drive_oil_per_wtg] ASC`;

    const result = await db.query(query, [wtgModel]);
    return result.length > 0 ? result[0] : null;
}

// Function to insert data into the database
async function insertData(db, result) {
    const insertQuery = `INSERT INTO gb_oil_change (SERVICE_ORDER_NUMBER, FUNCTIONAL_LOCATION, QUANTITY, TOTAL_QUANTITY, ISSUE, RETURN, PERCENTAGE_ISSUE_RETURN, CLASSIFICATION, STATE, AREA, SITE, WTG_Model, STOR_LOC, DOCUMENT_NUMBER, MATERIAL, MATERIAL_DESCRIPTION, PLANT, MOVE_TYPE, VAL_TYPE, POSTING_DATE, ENTRY_DATE, ZTEXT1, current_Oil_change_date, ORDER_TYPE) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

    const values = [
        result.SERVICE_ORDER_NUMBER,
        result.FUNCTIONAL_LOCATION,
        result.QUANTITY,
        result.TOTAL_QUANTITY,
        result.ISSUE,
        result.RETURN,
        result.PERCENTAGE_ISSUE_RETURN,
        result.CLASSIFICATION,
        result.STATE,
        result.AREA,
        result.SITE,
        result.WTG_Model,
        result.STOR_LOC,
        result.DOCUMENT_NUMBER,
        result.MATERIAL,
        result.MATERIAL_DESCRIPTION,
        result.PLANT,
        result.MOVE_TYPE,
        result.VAL_TYPE,
        result.POSTING_DATE,
        result.ENTRY_DATE,
        result.ZTEXT1,
        result.current_Oil_change_date,
        result.ORDER_TYPE
    ];

    await db.query(insertQuery, values);
}

// Start processing
(async () => {
    const db = await connectToDatabase();
    await processAndInsertData(db);
})();
