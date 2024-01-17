#include <stdio.h>

#include "json/json.h"
#include "curl/curl.h"

#include "Stock.h"
#include "Util.h"
#include "Database.h"
#include "MarketData.h"

int main(void)
{
    string database_name = "MarketData.db";
    cout << "Opening MarketData.db ..." << endl;
    sqlite3* db = NULL;
    if (OpenDatabase(database_name.c_str(), db) == -1)
        return -1;
    
    bool bCompleted = false;
    char selection;
    map<string, Stock> stockMap;
    
    string sConfigFile = "config_mac.csv";
    map<string, string> config_map = ProcessConfigData(sConfigFile);
    string daily_url_common = config_map["daily_url_common"];
    string intraday_url_common = config_map["intraday_url_common"];
    string start_date = config_map["start_date"];
    string end_date = config_map["end_date"];
    string api_token = config_map["api_token"];
    string symbol = config_map["symbol"];
    char delimiter = ',';
    vector<string> tickers;
    string ticker;
    istringstream tickerStream(symbol);
    tm tm1 = {};
    tm tm2 = {};
    istringstream s1(start_date);
    s1 >> get_time(&tm1, "%Y-%m-%d");
    time_t UNIX_TIMESTAMP_FROM = mktime(&tm1);
    time_t timezone_difference_1 = std::difftime(UNIX_TIMESTAMP_FROM, timegm(&tm1));
    UNIX_TIMESTAMP_FROM = UNIX_TIMESTAMP_FROM - timezone_difference_1;
    istringstream s2(end_date);
    s2 >> get_time(&tm2, "%Y-%m-%d");
    time_t UNIX_TIMESTAMP_TO = mktime(&tm2);
    time_t timezone_difference_2 = std::difftime(UNIX_TIMESTAMP_TO, timegm(&tm2));
    UNIX_TIMESTAMP_TO = UNIX_TIMESTAMP_TO - timezone_difference_2;
//    cout << UNIX_TIMESTAMP_FROM << endl << UNIX_TIMESTAMP_TO << endl;
    string from_str = to_string(UNIX_TIMESTAMP_FROM);
    string to_str = to_string(UNIX_TIMESTAMP_TO);
    string daily_url_request, intraday_url_request;
    
    while (getline(tickerStream, ticker, delimiter))
    {
        tickers.push_back(ticker);
    }
    
    while (!bCompleted)
    {
        cout << endl;
        cout << "Menu" << endl;
        cout << "========" << endl;
        cout << "A - Create DailyTrades and IntradayTrades Tables" << endl;
        cout << "B - Retrieve Daily Trade and Intraday Trade Data" << endl;
        cout << "C - Populate DailyTrades and IntradayTrades Table" << endl;
        cout << "D - Retrieve Data from DailyTrades and IntradayTrades Tables" << endl;
        cout << "X - Exit" << endl << endl;
        cout << "Enter selection: ";
        std::cin >> selection;
        switch (toupper(selection))
        {
            case 'A':
            {
                // Drop the table if exists
                cout << "Drop DailyTrades table if exists" << endl;
                string sql_DropaTable = "DROP TABLE IF EXISTS DailyTrades";
                if (DropTable(db, sql_DropaTable.c_str()) == -1)
                    return -1;
                
                // Create the table
                cout << "Creating DailyTrades table ..." << endl;
                string sql_CreateTable = string("CREATE TABLE IF NOT EXISTS DailyTrades ")
                + "(Symbol CHAR(20) NOT NULL,"
                + "Date CHAR(20) NOT NULL,"
                + "Open REAL NOT NULL,"
                + "High REAL NOT NULL,"
                + "Low REAL NOT NULL,"
                + "Close REAL NOT NULL,"
                + "Adjusted_close REAL NOT NULL,"
                + "Volume INT NOT NULL,"
                + "PRIMARY KEY(Symbol, Date),"
                + "Foreign Key(Symbol) references Stocks(Symbol)\n"
                + "ON DELETE CASCADE\n"
                + "ON UPDATE CASCADE"
                + ");";
                
                if (ExecuteSQL(db, sql_CreateTable.c_str()) == -1)
                    return -1;
                
                cout << "Drop IntradayTrades table if exists" << endl;
                sql_DropaTable = "DROP TABLE IF EXISTS IntradayTrades";
                if (DropTable(db, sql_DropaTable.c_str()) == -1)
                    return -1;
                
                cout << "Creating IntradayTrades table ..." << endl;
                sql_CreateTable = string("CREATE TABLE IF NOT EXISTS IntradayTrades ")
                + "(Symbol CHAR(20) NOT NULL,"
                + "Date CHAR(20) NOT NULL,"
                + "Timestamp CHAR(20) NOT NULL,"
                + "Open REAL NOT NULL,"
                + "High REAL NOT NULL,"
                + "Low REAL NOT NULL,"
                + "Close REAL NOT NULL,"
                + "Volume INT NOT NULL,"
                + "PRIMARY KEY(Symbol, Date, Timestamp),"
                + "Foreign Key(Symbol, Date) references DailyTrades(Symbol, Date)\n"
                + "ON DELETE CASCADE\n"
                + "ON UPDATE CASCADE\n"
                + ");";
                
                if (ExecuteSQL(db, sql_CreateTable.c_str()) == -1)
                    return -1;
                break;
            }
            case 'B':
            {
                for(int i = 0;i < 3;i++)
                {
                    daily_url_request = daily_url_common + tickers[i] + ".US?" + "from=" + start_date + "&to=" + end_date + "&api_token=" + api_token + "&period=d&fmt=json";
                    string readBuffer;
                    if (PullMarketData(daily_url_request, readBuffer) != 0)
                        return -1;
                }
                
                for(int i = 0;i < 3;i++)
                {
                    intraday_url_request = intraday_url_common + tickers[i] + ".US?" + "from=" + from_str + "&to=" + to_str + "&api_token=" + api_token + "&interval=5m&fmt=json";
                    string readBuffer;
                    if (PullMarketData(intraday_url_request, readBuffer) != 0)
                        return -1;
                }
                break;
            }
            case 'C':
            {
                for(int i = 0;i < 3;i++)
                {
                    daily_url_request = daily_url_common + tickers[i] + ".US?" + "from=" + start_date + "&to=" + end_date + "&api_token=" + api_token + "&period=d&fmt=json";
                    string readBuffer;
                    
                    //global initiliation of curl before calling a function
                    curl_global_init(CURL_GLOBAL_ALL);
                    
                    //creating session handle
                    CURL * handle;
                    
                    // Store the result of CURL’s webpage retrieval, for simple error checking.
                    CURLcode result;
                    
                    handle = curl_easy_init();
                    
                    if (!handle)
                    {
                        cout << "curl_easy_init failed" << endl;
                        return -1;
                    }
                    
                    curl_easy_setopt(handle, CURLOPT_URL, daily_url_request.c_str());
                    
                    //adding a user agent
                    curl_easy_setopt(handle, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0");
                    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0);
                    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 0);
                    
                    // send all data to this function
                    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, WriteCallback);
                    
                    // we pass our 'chunk' struct to the callback function
                    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &readBuffer);
                    
                    //perform a blocking file transfer
                    result = curl_easy_perform(handle);
                    
                    // check for errors
                    if (result != CURLE_OK) {
                        fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(result));
                    }
                    else
                    {
                        //json parsing
                        Json::Reader reader;
                        Json::Value root;   // will contains the root value after parsing.
                        
                        bool parsingSuccessful = reader.parse(readBuffer.c_str(), readBuffer.c_str() + readBuffer.size(), root);
                        if (!parsingSuccessful)
                        {
                            // Report failures and their locations in the document.
                            cout << "Failed to parse JSON: " << reader.getFormattedErrorMessages();;
                            return -1;
                        }
                        else
                        {
                            cout << "\nSucess parsing json\n" << endl;
                            
                            string symbol;
                            if (daily_url_request.find("MSFT") != std::string::npos)
                                symbol = "MSFT";
                            else if (daily_url_request.find("IBM") != std::string::npos)
                                symbol = "IBM";
                            else if (daily_url_request.find("GOOGL") != std::string::npos)
                                symbol = "GOOGL";
                            
                            string date;
                            float open, high, low, close, adjusted_close;
                            int volume;
                            
                            
                            for (Json::Value::const_iterator itr = root.begin(); itr != root.end(); itr++)
                            {
                                Stock myStock(symbol);
                                date = (*itr)["date"].asString();
                                open = (*itr)["open"].asFloat();
                                high = (*itr)["high"].asFloat();
                                low = (*itr)["low"].asFloat();
                                close = (*itr)["close"].asFloat();
                                adjusted_close = (*itr)["adjusted_close"].asFloat();
                                volume = (*itr)["volume"].asInt();
                                DailyTrade aTrade(date, open, high, low, close, adjusted_close, volume);
                                myStock.addDailyTrade(aTrade);
                                if (PopulateDailyTrades(readBuffer, myStock) != 0)
                                    return -1;
                            }
                            
                        }
                    }
                    curl_easy_cleanup(handle);

                }
                
                for(int i = 0;i < 3;i++)
                {
                    intraday_url_request = intraday_url_common + tickers[i] + ".US?" + "from=" + from_str + "&to=" + to_str + "&api_token=" + api_token + "&interval=5m&fmt=json";
                    
                    string readBuffer;
                    
                    //global initiliation of curl before calling a function
                    curl_global_init(CURL_GLOBAL_ALL);
                    
                    //creating session handle
                    CURL * handle;
                    
                    // Store the result of CURL’s webpage retrieval, for simple error checking.
                    CURLcode result;
                    
                    handle = curl_easy_init();
                    
                    if (!handle)
                    {
                        cout << "curl_easy_init failed" << endl;
                        return -1;
                    }
                    
                    curl_easy_setopt(handle, CURLOPT_URL, intraday_url_request.c_str());
                    
                    //adding a user agent
                    curl_easy_setopt(handle, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0");
                    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0);
                    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 0);
                    
                    // send all data to this function
                    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, WriteCallback);
                    
                    // we pass our 'chunk' struct to the callback function
                    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &readBuffer);
                    
                    //perform a blocking file transfer
                    result = curl_easy_perform(handle);
                    
                    // check for errors
                    if (result != CURLE_OK) {
                        fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(result));
                    }
                    else
                    {
                        //json parsing
                        Json::Reader reader;
                        Json::Value root;   // will contains the root value after parsing.
                        
                        bool parsingSuccessful = reader.parse(readBuffer.c_str(), readBuffer.c_str() + readBuffer.size(), root);
                        if (!parsingSuccessful)
                        {
                            // Report failures and their locations in the document.
                            cout << "Failed to parse JSON: " << reader.getFormattedErrorMessages();;
                            return -1;
                        }
                        else
                        {
                            cout << "\nSucess parsing json\n" << endl;
                            
                            string symbol;
                            if (intraday_url_request.find("MSFT") != std::string::npos)
                                symbol = "MSFT";
                            else if (intraday_url_request.find("IBM") != std::string::npos)
                                symbol = "IBM";
                            else if (intraday_url_request.find("GOOGL") != std::string::npos)
                                symbol = "GOOGL";
                            
                            long timestamp;
                            float gmtoffset;
                            string date, time;
//                            char datetime[128];
                            float open, high, low, close;
                            int volume;
                            long hours, minutes, seconds;
                            char delimiter;
                            
                            for (Json::Value::iterator itr = root.begin(); itr != root.end(); itr++)
                            {
                                Stock myStock(symbol);
                                timestamp = (*itr)["timestamp"].asInt64();
                                gmtoffset = (*itr)["gmtoffset"].asFloat();
                                date = (*itr)["datetime"].asString();
                                open = (*itr)["open"].asFloat();
                                high = (*itr)["high"].asFloat();
                                low = (*itr)["low"].asFloat();
                                close = (*itr)["close"].asFloat();
                                volume = (*itr)["volume"].asInt();
                                time = date.substr(11, 8);
                                istringstream s1(time);
                                s1 >> hours >> delimiter >> minutes >> delimiter >> seconds;
                                timestamp = (hours - 4) * 3600 + minutes * 60 + seconds;
                                seconds = timestamp;
                                hours = seconds / 3600;
                                seconds %= 3600;
                                minutes = seconds / 60;
                                seconds %= 60;
                                ostringstream oss;
                                oss << std::setw(2) << std::setfill('0') << hours << ":"
                                    << std::setw(2) << std::setfill('0') << minutes << ":"
                                    << std::setw(2) << std::setfill('0') << seconds;
                                date = date.substr(0, 10);
                                IntradayTrade aTrade(date, oss.str().c_str(), open, high, low, close, volume);
                                myStock.addIntradayTrade(aTrade);
                                if (PopulateIntradayTrades(readBuffer, myStock, timestamp) != 0)
                                    return -1;
                            }
                        }
                    }
                    curl_easy_cleanup(handle);
                }
                break;
            }
            case 'D':
            {
                // Display DailyTrades Table
                cout << "Retrieving values in table DailyTrades ..." << endl;
                char sql_Select[512];
                snprintf(sql_Select, sizeof(sql_Select), "SELECT * FROM DailyTrades;");
                if (ShowTable(db, sql_Select) == -1)
                    return -1;
                cout << "Retrieving values in table IntradayTrades ..." << endl;
                snprintf(sql_Select, sizeof(sql_Select), "SELECT * FROM IntradayTrades;");
                if (ShowTable(db, sql_Select) == -1)
                    return -1;
                break;
            }
            case 'X':
            {
                bCompleted = true;
                break;
            }
            default:
            {
                break;
            }
        }
    }
    return 0;
}

