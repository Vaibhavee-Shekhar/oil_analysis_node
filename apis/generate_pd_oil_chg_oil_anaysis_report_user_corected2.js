const express = require('express');
const odbc = require('odbc'); // For database connection
const ExcelJS = require('exceljs'); // For Excel file operations
const cors = require('cors'); // For enabling CORS

const app = express();
app.use(cors());

// Helper function: Database connection setup
async function connectToDatabase() {
    const connectionString = 'Driver={SQL Server};Server=YOUR_SERVER_NAME;Database=YOUR_DATABASE_NAME;Trusted_Connection=yes;';
    const connection = await odbc.connect(connectionString);
    return connection;
}

// Function to fetch data in chunks
async function fetchDataInBatches(connection, query, batchSize = 2000) {
    let offset = 0;
    let results = [];
    let hasMoreData = true;

    while (hasMoreData) {
        const paginatedQuery = `${query} OFFSET ${offset} ROWS FETCH NEXT ${batchSize} ROWS ONLY`;
        const batchResults = await connection.query(paginatedQuery);
        results = results.concat(batchResults);
        offset += batchSize;
        hasMoreData = batchResults.length === batchSize;
    }
    return results;
}

// Function to process data
async function processResults(results, connection) {
    const finalData = {};
    const orderResults = [];
    const validMaterials = ['51028446'];

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
                    current_Oil_change_date: row.current_Oil_change_date || null,
                    order_type: row.order_type,
                    material_description: row.TXTMD,
                    stor_loc: row.STOR_LOC,
                    document_number: row.DOCUMENT_NUMBER,
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
        const ret = data.return;
        const absIssue = Math.abs(issue);

        const percentageIssueReturn = issue !== 0 ? Math.abs((ret / issue) * 100) : 0;
        const functionalLocation = data.functional_location;

        const wtgQuery = `
            SELECT [WTG_Model], [State], [Area], [Site]
            FROM [NewDatabase].[dbo].[installedbase]
            WHERE [Functional_Location] = ?
        `;
        const wtgStmt = await connection.query(wtgQuery, [functionalLocation]);
        const installedBaseResult = wtgStmt[0];

        if (installedBaseResult) {
            const { WTG_Model: wtgModel, State: state, Area: area, Site: site } = installedBaseResult;

            if (data.order_type.startsWith('PD_OIL_CHG_ORDER')) {
                if (percentageIssueReturn > 80 && percentageIssueReturn < 100) {
                    orderResults.push({
                        SERVICE_ORDER_NUMBER: serviceOrderNumber,
                        FUNCTIONAL_LOCATION: functionalLocation,
                        QUANTITY: absIssue + Math.abs(ret),
                        ISSUE: issue,
                        RETURN: ret,
                        PERCENTAGE_ISSUE_RETURN: percentageIssueReturn,
                        Order: 'PD_OIL_CHG_ORDER',
                        STATE: state,
                        AREA: area,
                        SITE: site,
                        WTG_Model: wtgModel,
                        STOR_LOC: data.stor_loc,
                        DOCUMENT_NUMBER: data.document_number,
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
                        Component: 'PD',
                    });
                }
            }
        }
    }

    return orderResults;
}

// API Route to process data
app.get('/process-data', async (req, res) => {
    try {
        const connection = await connectToDatabase();

        const query = `
        WITH ServiceOrders AS (
            SELECT *, 
                   CASE 
                       WHEN [MATERIAL] IN ('51028446') 
                       THEN 1 ELSE 0 END AS material_in_list
            FROM [NewDatabase].[dbo].[consumption_analysis_table]
            WHERE NOT (order_type LIKE 'GB%' OR order_type LIKE 'FC%' OR order_type LIKE 'yd%')
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

        const results = await fetchDataInBatches(connection, query);
        const processedResults = await processResults(results, connection);

        res.json(processedResults);
        await connection.close();
    } catch (err) {
        console.error('Error:', err);
        res.status(500).json({ error: 'An error occurred while processing the data.' });
    }
});

// Start the Express server
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
