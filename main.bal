// RESTful API with for a online rewards system that integrs
// with a MySQL database to store user information and transactions
import ballerina/io;
import ballerina/log;
import ballerina/http;
import ballerina/random;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerina/sql;
import ballerinax/trigger.shopify as shopify;
import ballerinax/shopify.admin as shopifyAdmin;

// Configurable variable for Webhook URL
configurable string WEBHOOK_URL = "https://17eb-49-36-208-65.ngrok-free.app";
configurable string STORE_URL = "https://storefront-exe.myshopify.com";

// Configurable variable for conversion value
configurable int CONVERSION_VALUE = 100; // default: 100 points = 1 INR Spent 


// Configurable variables for the Shopify trigger
configurable shopify:ListenerConfig listenerConfig = {
    apiSecretKey: ""
};

// Configurable variables for the Shopify admin client
configurable shopifyAdmin:ApiKeysConfig config = {
    xShopifyAccessToken: ""
};
shopifyAdmin:Client shopifyClient = check new (config, STORE_URL);

// Configurable variables for the MySQL database
configurable string DB_HOST = "localhost";
configurable string DB_USER = "root";
configurable string DB_PWD = "root";
configurable string DB_SCHEMA = "rewards_db";
configurable int DB_PORT = 3306;

// Table schema for the user table
// Table Users - id, name, email, points
// Table Transactions - order_id, user_id, operation_type, value, created_at
type User record {
    int id;
    string name;
    string email;
    int points;
};

type Transaction record {
    int order_id?;
    string order_name?;
    int user_id?;
    string operation_type?; // Accepted values: "credit", "redeem", "manual_credit", "manual_debit"
    int value?;
};

final mysql:Client dbClient = check new (DB_HOST, DB_USER, DB_PWD, DB_SCHEMA, DB_PORT);

# HELPER FUNCTION TO GET USER DETAILS AND ORDER PRICE FROM SHOPIFY ORDER
# + Order - Webhook ID
# + return - Success message or error message
isolated function getDetailsForOrderData(json Order) returns json|error {
    int user_id = check Order.customer.id;
    string user_name = check Order.customer.first_name;
    string user_email = check Order.customer.email;
    int order_id = check Order.id;
    string order_name = check Order.name;
    int order_price = check float:fromString(check Order.total_price).ensureType(int);

    io:println({
        "user_id": user_id,
        "user_name": user_name,
        "user_email": user_email,
        "order_id": order_id,
        "order_name": order_name,
        "order_price": order_price
    },"\n\n");

    return {
        "user_id": user_id,
        "user_name": user_name,
        "user_email": user_email,
        "order_id": order_id,
        "order_name": order_name,
        "order_price": order_price
    };

}

# HELPER FUNCTION TO CHECK IF USER EXISTS 
# + user_id - User ID
# + return - Boolean value or error message
isolated function checkUser(int user_id) returns boolean|error {
    sql:ParameterizedQuery selectQuery = `SELECT * FROM users WHERE id = ${user_id}`;
    stream<User, sql:Error?> query_stream = dbClient->query(selectQuery);
    check from User user in query_stream
        do {
            log:printInfo("checkUser: User" + user_id.toString() + " exists.");
            return true;
        };
    return false;
}

# HELPER FUNCTION TO CREATE A NEW USER
# + user - User object
# + return - Success message or error message
isolated function createUser(User user) returns json|error {
    sql:ParameterizedQuery insertQuery = `INSERT INTO users (id, name, email, points) VALUES (${user.id}, "${user.name}", "${user.email}", 0)`;
    
    io:println("---",insertQuery);
    
    sql:ExecutionResult|error result = dbClient->execute(insertQuery);
    if (result is error) {
        log:printError("createUser: User creation failed." + result.toString());
        return error("User creation failed.");
    } else {
        log:printInfo("createUser: User created successfully." + result.toString());
        return { "message": "User created successfully." };
    }
}

# FUNCTION TO UPDATE THE TRANSACTION TABLE
# + transact - Transaction object
# + return - Success message or error message
isolated function updateTransactionTable(Transaction transact) returns json|error {
        log:printInfo("updateTransactionTable: Updating transaction table for order ID: " + transact.order_id.toString() + " User ID: " + transact.user_id.toString() + " Value: " + transact.value.toString() + " Operation: " + <string>transact.operation_type);

        int points = <int>transact.value/CONVERSION_VALUE;
        // insert the transaction into the transactions table
        sql:ParameterizedQuery insertTransactionQuery = `INSERT INTO transactions (order_id, order_name, user_id, operation_type, value, created_at) VALUES (${transact.order_id}, ${transact.order_name}, ${transact.user_id}, ${transact.operation_type}, ${points}, CURRENT_TIMESTAMP())`;
        io:println(insertTransactionQuery);


        sql:ExecutionResult|error transact_table_result = dbClient->execute(insertTransactionQuery);
            
        if (transact_table_result is error) {
            log:printError("updateTransactionTable: Transaction failed. Error occurred while inserting transaction." + transact_table_result.toString());
            return error("Transaction filed. Error occurred while inserting transaction.");
        }

        log:printInfo("updateTransactionTable: Transaction successful." + transact_table_result.toString());
        return { 
            "message": "Transaction successful."         
        };

    
}

# FUNCTION TO UPDATE THE USER TABLE
# + user_id - User ID
# + value - Value to be updated
# + operation - Operation type
# + return - Success message or error message
isolated function updateUserTable(int user_id, int value, string operation) returns json|error {
    log:printInfo("updateUserTable: Updating user table for user ID: " + user_id.toString() + " Value: " + value.toString() + " Operation: " + operation);
    int current_points;
    int updated_points = 0;

    // Query the user table to get the current points
    sql:ParameterizedQuery pointsQuery = `SELECT points FROM users WHERE id = ${user_id}`;
    io:println(pointsQuery);
    stream<User, sql:Error?> pointsQuery_stream = dbClient->query(pointsQuery);

    check from User user in pointsQuery_stream
        do {
            current_points = user.points;
        };

    io:println("Current Pts: ", current_points, " Value: ", value);
    
    match operation {
        "credit" => {
            updated_points = current_points + value/CONVERSION_VALUE;
        }

        "redeem" => {
            io:println("Redeem");
            if value > current_points {
                log:printError("Insufficient points" + " " + user_id.toString() + value.toString() + " " + current_points.toString());                 
                return error("Insufficient points");
            } else {
                updated_points = current_points - value/CONVERSION_VALUE;
            }
        }

        "manual_credit" => {
            io:println("Manual Credit");
            updated_points = current_points + value/CONVERSION_VALUE;
        }

        "manual_debit" => {
            io:println("Manual Debit");
            if value > current_points {  
                log:printError("Insufficient points" + " " + user_id.toString() + value.toString() + " " + current_points.toString());                               
                return error("Insufficient points");
            } else {
                updated_points = current_points - value/CONVERSION_VALUE;
            }   
        }
    }
    

    // update the user points in user table
    sql:ParameterizedQuery setPointsQuery = `UPDATE users SET points = ${updated_points} WHERE id = ${user_id}`;
    io:println(setPointsQuery);
    
    sql:ExecutionResult|error user_table_result = dbClient->execute(setPointsQuery);
    
    if (user_table_result is error) {
        log:printError("updateUserTable: Transaction filed. Error occurred while updating user points." + user_table_result.toString());
        return error("Transaction filed. Error occurred while updating user points.");
    }

    log:printInfo("updateUserTable: Transaction successful." + user_table_result.toString());
    return { "message": "Transaction successful.", "current_points": current_points };
}

# HELPER FUNCTION TO GENERATE A RANDOM DISCOUNT CODE
# + length - Length of the discount code
# + return - Random discount code
isolated function generateDiscountCode(int length) returns string|error {
    string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    string discountCode = "";
    foreach int i in 0...length {
        int randomIndex = check random:createIntInRange(1, chars.length());
        discountCode = discountCode + chars[randomIndex];
        
    }
    return discountCode;
}


# CREATE A SHOPIFY DISCOUNT CODE
# + discount_value - Discount value
# + return - Success message or error message
isolated function createDiscountCode(string user_id, int discount_value) returns json|error {
    log:printInfo("createDiscountCode: Creating discount code for user: " + user_id  + " Discount Value: " + discount_value.toString());
    
    final http:Client shopifyHttpClient = check new (STORE_URL);
    final map<string[]> headers = {
            "X-Shopify-Access-Token": ["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"],
            "Content-Type": ["application/json"]
    };

    final string priceRuleUrl = "/admin/api/2024-01/price_rules.json";
    final json priceRulePayload = {
        "price_rule": {
            "title": "Test1234",
            "value_type": "fixed_amount",
            "value":  0 - discount_value,
            "customer_selection": "prerequisite",
            "prerequisite_customer_ids": [user_id],
            "target_type": "line_item",
            "target_selection": "all",
            "allocation_method": "across",
            "allocation_limit": 1,
            "starts_at": "2024-01-01T00:00:00Z",
            "once_per_customer": true


        }
    };
    // create price rule
    http:Response shopifyHttpClientRequest = check shopifyHttpClient->post(priceRuleUrl, priceRulePayload, headers);
    // get price rule id 
    string priceRuleId = ""; 

    if shopifyHttpClientRequest is http:Response {
        final json response = check shopifyHttpClientRequest.getJsonPayload();
        if (response.price_rule is json) {
            priceRuleId =  (check response.price_rule.id).toString();
        }       
    }
    log:printInfo("createDiscountCode: Price Rule created successfully.");


    // create discount code
    string discountCode = check generateDiscountCode(12);
    final string discountCodeUrl = "/admin/api/2024-01/price_rules/" + priceRuleId + "/discount_codes.json";
    final json discountCodePayload = {
        "discount_code": {
            "code": discountCode
        }
    };
    shopifyHttpClientRequest = check shopifyHttpClient->post(discountCodeUrl, discountCodePayload, headers);
    io:println(shopifyHttpClientRequest.getJsonPayload());

}

# Shopify listener service to handle order related events
listener shopify:Listener shopifyListener = new(listenerConfig, 8090);

service shopify:OrdersService on shopifyListener {
    remote function onOrdersCreate(shopify:OrderEvent event) returns error? {
        // print order details
        log:printInfo("onOrdersCreate: New Order Event: " + event.toString());
        // io:println(event.toJson(),"\n\n");
        json|error orderData = getDetailsForOrderData(event.toJson());
        // io:println(orderData,"\n\n");
        
        if orderData is error {
            log:printError("onOrdersCreate: Not sufficient data in the order object to process the order.");
            return;
        }

        // check if the user exists. If not, create a new user
        if (!check checkUser(<int> check orderData.user_id)) {
            json|error user = createUser({
                id: check orderData.user_id,
                name: check orderData.user_name,
                email: check orderData.user_email,
                points: 0
            });

            if user is error {
                log:printError("onOrdersCreate: Transaction failed. Error occurred while creating user.");
                return;
            }
        }

        
        Transaction transact = {
            order_id: check orderData.order_id,
            order_name: check orderData.order_name,
            user_id: check orderData.user_id,
            operation_type: "credit",
            value: check orderData.order_price
        };

        // update the user table with the transaction details
        json|error updateUser = updateUserTable(<int>transact.user_id, <int>transact.value, <string>transact.operation_type);
        if updateUser is error {
            log:printError("onOrdersCreate: Transaction failed. Error occurred while updating user points.");
            return;
        }

        // insert the transaction into the transactions table
        json|error updateTransaction = updateTransactionTable(transact);
        if updateTransaction is error {
            log:printError("onOrdersCreate: Transaction failed. Error occurred while inserting transaction.");
            return;
        }
        
    }

    remote function onOrdersCancelled(shopify:OrderEvent event) returns error? {
        // io:println(event.toJson());
        log:printInfo("onOrdersCancelled: Order Cancelled Event: " + event.toString());
        json|error orderData = getDetailsForOrderData(event.toJson());
        
        if orderData is error {
            log:printError("onOrdersCancelled: Not sufficient data in the order object to process the order.");
            return;
        }
        
        Transaction transact = {
            order_id: check orderData.order_id,
            order_name: check orderData.order_name,
            user_id: check orderData.user_id,
            operation_type: "manual_debit",
            value: check orderData.order_price
        };

        // update the user table with the transaction details
        json|error updateUser = updateUserTable(<int>transact.user_id, <int>transact.value, <string>transact.operation_type);
        if updateUser is error {
            log:printError("onOrdersCancelled: Transaction failed. Error occurred while updating user points.");
            return;
        }

        // insert the transaction into the transactions table
        json|error updateTransaction = updateTransactionTable(transact);
        if updateTransaction is error {
            log:printError("onOrdersCancelled: Transaction failed. Error occurred while inserting transaction.");
            return;
        }
        
    }

    remote function onOrdersFulfilled(shopify:OrderEvent event) returns error? {
        log:printInfo("onOrdersFulfilled: Order Fulfilled Event: " + event.toString());
        return;
    }

    remote function onOrdersPaid(shopify:OrderEvent event) returns error? {
        log:printInfo("onOrdersPaid: Order Paid Event: " + event.toString());
        return;
    }

    remote function onOrdersPartiallyFulfilled(shopify:OrderEvent event) returns error? {
        log:printInfo("onOrdersPartiallyFulfilled: Order Partially Fulfilled Event: " + event.toString());
        return;
    }

    remote function onOrdersUpdated(shopify:OrderEvent event) returns error? {
        log:printInfo("onOrdersUpdated: Order Updated Event: " + event.toString());
        return;
    }
}



# Service to handle user related operations - create, get info, get transactions, redeem points, credit points
service /users on new http:Listener(9090) {
    # CREATE A NEW USER
    # + user - User object
    # + return - Success message or error message
    resource function post create(User user) returns json|error {
        sql:ParameterizedQuery insertQuery = `INSERT INTO users (id, name, email, points) VALUES (${user.id}, ${user.name}, ${user.email}, 0)`;
        sql:ExecutionResult|error result = dbClient->execute(insertQuery);
        if (result is error) {
            return { "error": "User creation failed." };
        } else {
            return { "message": "User created successfully." };
        }
    }
    
    
    # GET CURRENT USER STATS - ID, NAME, EMAIL, POINTS
    # + user - User object
    # + return - User object or error message
    resource function get info(User user) returns User|error {
        int? user_id = user.id;
        sql:ParameterizedQuery selectQuery = `SELECT * FROM user WHERE user_id = ${user_id}`;
        // sql:ExecutionResult|error result = dbClient->execute(selectQuery);

        stream<User, sql:Error?> query_stream = dbClient->query(selectQuery);
        check from User u in query_stream
            do {
                return u;
            };
        return error("Error occurred while retrieving user information.");
    }

    # GET ALL TRANSACTIONS FOR A USER
    # + user_id - User ID
    # + return - Array of transactions or error message
    resource function get transactions(int user_id) returns Transaction[]|error {
        sql:ParameterizedQuery selectQuery = `SELECT * FROM transactions WHERE user_id = ${user_id}`;
        // sql:ExecutionResult|error result = dbClient->execute(selectQuery);

        stream<Transaction, sql:Error?> query_stream = dbClient->query(selectQuery);
        Transaction[] transactions = [];
        check from Transaction transact in query_stream
            do {
                // io:println(transact.toJson());
                transactions.push(transact);    
            };

        return transactions;
    }

    # REDEEM POINTS FOR A USER
    # + transact - Transaction object
    # + return - Success message or error message
    resource function post redeem(Transaction transact) returns json|error {
        int? value = transact.value;
        int current_points;

        // Query the user table to get the current points
        sql:ParameterizedQuery pointsQuery = `SELECT points FROM users WHERE id = ${transact.user_id}`;
        io:println(pointsQuery);
        stream<User, sql:Error?> pointsQuery_stream = dbClient->query(pointsQuery);

        check from User user in pointsQuery_stream
            do {
                current_points = user.points;
            };

        io:println("Current Pts: ", current_points);
        
        if value is int{
            if value > current_points {                 
                return { "error": "Insufficient points" };
            } else {
                current_points -= value;
            }
        }
      

        // update the user points in user table
        sql:ParameterizedQuery setPointsQuery = `UPDATE users SET points = ${current_points} WHERE id = ${transact.user_id}`;
        io:println(setPointsQuery);

        // insert the transaction into the transactions table
        sql:ParameterizedQuery insertTransactionQuery = `INSERT INTO transactions (order_id, order_name, user_id, operation_type, value, created_at) VALUES (${transact.order_id}, ${transact.order_name}, ${transact.user_id}, ${transact.operation_type}, ${transact.value}, CURRENT_TIMESTAMP())`;
        io:println(insertTransactionQuery);


        sql:ExecutionResult|error user_table_result = dbClient->execute(setPointsQuery);
        if (user_table_result is error) {
            return { "error": "Transaction filed. Error occurred while updating user points." };
        } else {

            sql:ExecutionResult|error transact_table_result = dbClient->execute(insertTransactionQuery);
            
            if (transact_table_result is error) {
                return { "error": "Transaction filed. Error occurred while inserting transaction." };
            } else {
                 // generate discount code
                json|error discountCode = createDiscountCode(transact.user_id.toString(), <int>(<int>transact.value/CONVERSION_VALUE));
                if discountCode is json {
                    return { 
                        "message": "Transaction successful.",
                        "current_points": current_points,
                        "discount_code": check ((discountCode.toJson()).discount_code.code)
                    };
                }
            }

        }

        
    }

    # CREDIT POINTS FOR A USER
    # + transact - Transaction object
    # + return - Success message or error message
    resource function put credit(Transaction transact) returns json|error {
        int? value = transact.value;
        int current_points;

        // Query the user table to get the current points
        sql:ParameterizedQuery pointsQuery = `SELECT points FROM users WHERE id = ${transact.user_id}`;
        io:println(pointsQuery);
        stream<User, sql:Error?> pointsQuery_stream = dbClient->query(pointsQuery);

        check from User user in pointsQuery_stream
            do {
                current_points = user.points;
            };

        io:println("Current Pts: ", current_points);
        
        if value is int{
            current_points -= value;
        }
      

        // update the user points in user table
        sql:ParameterizedQuery setPointsQuery = `UPDATE users SET points = ${current_points} WHERE id = ${transact.user_id}`;
        io:println(setPointsQuery);

         // insert the transaction into the transactions table
        sql:ParameterizedQuery insertTransactionQuery = `INSERT INTO transactions (order_id, order_name, user_id, operation_type, value, created_at) VALUES (${transact.order_id}, ${transact.order_name}, ${transact.user_id}, ${transact.operation_type}, ${transact.value}, CURRENT_TIMESTAMP())`;
        io:println(insertTransactionQuery);


        sql:ExecutionResult|error user_table_result = dbClient->execute(setPointsQuery);
        if (user_table_result is error) {
            return { "error": "Transaction filed. Error occurred while updating user points." };
        } else {

            sql:ExecutionResult|error transact_table_result = dbClient->execute(insertTransactionQuery);
            
            if (transact_table_result is error) {
                return { "error": "Transaction filed. Error occurred while inserting transaction." };
            } else {
                return { 
                    "message": "Transaction successful.",
                    "current_points": current_points
                };
            }

        }
        
    }
}

# Service to handle transaction related operations - get recent transactions
service /transactions on new http:Listener(9091) {
    # GETS PAGINATED LIST OF RECENT TRANSACTIONS
    # + offset - Offset for the query
    # + limt - Limit for the query
    # + return - Array of transactions or error message
    resource function get records(int offset, int limt) returns Transaction[]|error {
        sql:ParameterizedQuery selectQuery = `SELECT * FROM transactions ORDER BY created_at DESC LIMIT ${offset}, ${limt}`;
        // sql:ExecutionResult|error result = dbClient->execute(selectQuery);

        stream<Transaction, sql:Error?> query_stream = dbClient->query(selectQuery);
        Transaction[] transactions = [];
        check from Transaction transact in query_stream
            do {
                transactions.push(transact);
        };
        return transactions;
    }
}


public function main() returns error? {

    // create database tables
    log:printInfo("Initializing database tables...");
    log:printInfo("Creating users table...");
    sql:ExecutionResult|error result = dbClient->execute(`CREATE TABLE IF NOT EXISTS users ( id bigint unsigned primary key, name varchar(100), email varchar(100), points int)`);
    if (result is error) {
        return result;
    }
    log:printInfo("Creating transactions table...");
    sql:ExecutionResult|error result2 = dbClient->execute(`CREATE TABLE IF NOT EXISTS transactions ( order_id bigint unsigned primary key, order_name varchar(100), user_id bigint unsigned, operation_type varchar(100), value bigint, created_at timestamp)`);
    if (result2 is error) {
        return result2;
    }
    log:printInfo("Database tables created successfully.");

    // create webhooks

    shopifyAdmin:Webhook OrderCreatewebhook = {
        topic: "orders/create",
        address: WEBHOOK_URL,
        format: "json"
    };

    shopifyAdmin:Webhook OrderCancelledwebhook = {
        topic: "orders/cancelled",
        address: WEBHOOK_URL,
        format: "json"
    };
    
    shopifyAdmin:CreateWebhook createOrderCreateWebhook = {
        webhook: OrderCreatewebhook
    };

    shopifyAdmin:CreateWebhook createOrderCancelledWebhook = {
        webhook: OrderCancelledwebhook
    };
    
    // io:println(webhook);
    log:printInfo("Creating webhooks...");
    
    log:printInfo("Creating Order Create Webhook...");
    shopifyAdmin:WebhookObject response = check shopifyClient->createWebhook(createOrderCreateWebhook);
    log:printInfo(response.toString());
    
    log:printInfo("Creating Order Cancelled Webhook...");
    response = check shopifyClient->createWebhook(createOrderCancelledWebhook);
    log:printInfo(response.toString());

    
    // Fetch all webhooks
    io:println("Getting webhooks...");
    shopifyAdmin:WebhookList webhooks = check shopifyClient->getWebhooks();
    log:printInfo(webhooks.toString());

    // json|error discountCode = createDiscountCode("7646525063331", 100);
    // if discountCode is json {
    //     io:println(discountCode.toJson());
    // }
}

  