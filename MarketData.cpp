#include "MarketData.h"
#include <stdio.h>

#include "json/json.h"
#include "curl/curl.h"

#include "Stock.h"
#include "Util.h"
#include "Database.h"



int PullMarketData(const std::string& url_request, std::string& read_buffer)
{
    //global initiliation of curl before calling a function
    curl_global_init(CURL_GLOBAL_ALL);
    
    //creating session handle
    CURL * handle;
    
    // Store the result of CURLís webpage retrieval, for simple error checking.
    CURLcode result;
    
    handle = curl_easy_init();
    
    if (!handle)
    {
        cout << "curl_easy_init failed" << endl;
        return -1;
    }
    
    curl_easy_setopt(handle, CURLOPT_URL, url_request.c_str());
    
    //adding a user agent
    curl_easy_setopt(handle, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0");
    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0);
    curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 0);
    
    // send all data to this function
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, WriteCallback);
    
    // we pass our 'chunk' struct to the callback function
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &read_buffer);
    
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
        
        bool parsingSuccessful = reader.parse(read_buffer.c_str(), read_buffer.c_str() + read_buffer.size(), root);
        if (!parsingSuccessful)
        {
            // Report failures and their locations in the document.
            cout << "Failed to parse JSON: " << reader.getFormattedErrorMessages();;
            return -1;
        }
        else
        {
            cout << "\nSucess parsing json\n" << root << endl;
            
            string symbol;
            if (url_request.find("MSFT") != std::string::npos)
                symbol = "MSFT";
            else if (url_request.find("IBM") != std::string::npos)
                symbol = "IBM";
            else if (url_request.find("GOOGL") != std::string::npos)
                symbol = "GOOGL";
            
            if (url_request.find("intraday") == std::string::npos)
            {
                string date;
                float open, high, low, close, adjusted_close;
                int volume;
                
                Stock myStock(symbol);
                for (Json::Value::const_iterator itr = root.begin(); itr != root.end(); itr++)
                {
                    date = (*itr)["date"].asString();
                    open = (*itr)["open"].asFloat();
                    high = (*itr)["high"].asFloat();
                    low = (*itr)["low"].asFloat();
                    close = (*itr)["close"].asFloat();
                    adjusted_close = (*itr)["adjusted_close"].asFloat();
                    volume = (*itr)["volume"].asInt();
                    DailyTrade aTrade(date, open, high, low, close, adjusted_close, volume);
                    myStock.addDailyTrade(aTrade);
                    
                    cout << aTrade << endl;
                }
            }
            else
            {
                long timestamp;
                float gmtoffset;
                string date;
                char datetime[128];
                float open, high, low, close;
                int volume;
                Stock myStock(symbol);
                for (Json::Value::iterator itr = root.begin(); itr != root.end(); itr++)
                {
                    timestamp = (*itr)["timestamp"].asInt64();
                    gmtoffset = (*itr)["gmtoffset"].asFloat();
                    date = (*itr)["datetime"].asString();
                    open = (*itr)["open"].asFloat();
                    high = (*itr)["high"].asFloat();
                    low = (*itr)["low"].asFloat();
                    close = (*itr)["close"].asFloat();
                    volume = (*itr)["volume"].asInt();
                    time_t t = timestamp;
                    struct tm* tm = localtime(&t);
                    snprintf(datetime, sizeof(datetime), "%s", asctime(tm));
                    IntradayTrade aTrade(datetime, to_string(timestamp), open, high, low, close, volume);
                    myStock.addIntradayTrade(aTrade);
                    cout << aTrade << endl;
                }
            }
        }
    }
    curl_easy_cleanup(handle);
    return 0;
}

int PopulateDailyTrades(const std::string& read_buffer, Stock& stock)
{
//    cout << "Inserting daily data for a stock into table DailyTrades ..." << endl << endl;
    char sql_Insert[512];

    snprintf(sql_Insert, sizeof(sql_Insert), "INSERT INTO DailyTrades(Symbol, Date, Open, High, Low, Close, Adjusted_close, Volume) VALUES(\"%s\", \"%s\", %f, %f, %f, %f, %f, %d)", stock.GetSymbol().c_str(), stock.GetDailyTrade()[0].GetDate().c_str(), stock.GetDailyTrade()[0].GetOpen(), stock.GetDailyTrade()[0].GetHigh(), stock.GetDailyTrade()[0].GetLow(), stock.GetDailyTrade()[0].GetClose(), stock.GetDailyTrade()[0].GetAdjustedClose(), stock.GetDailyTrade()[0].GetVolume());
    
    string database_name = "MarketData.db";
//    cout << "Opening MarketData.db ..." << endl;
    sqlite3* db = NULL;
    if (OpenDatabase(database_name.c_str(), db) == -1)
        return -1;
    if (ExecuteSQL(db, sql_Insert) == -1)
        return -1;
    return 0;
}

int PopulateIntradayTrades(const std::string& read_buffer, Stock& stock, long start_date)
{
    char sql_Insert[512];
    
    snprintf(sql_Insert, sizeof(sql_Insert), "INSERT INTO IntradayTrades(Symbol, Date, Timestamp, Open, High, Low, Close, Volume) VALUES(\"%s\", \"%s\", \"%s\", %f, %f, %f, %f, %d)", stock.GetSymbol().c_str(), stock.GetIntradayTrade()[0].GetDate().c_str(), stock.GetIntradayTrade()[0].GetTimestamp().c_str(), stock.GetIntradayTrade()[0].GetOpen(),stock.GetIntradayTrade()[0].GetHigh(), stock.GetIntradayTrade()[0].GetLow(), stock.GetIntradayTrade()[0].GetClose(), stock.GetIntradayTrade()[0].GetVolume());
    
    string database_name = "MarketData.db";
//    cout << "Opening MarketData.db ..." << endl;
    sqlite3* db = NULL;
    if (OpenDatabase(database_name.c_str(), db) == -1)
        return -1;
    if (ExecuteSQL(db, sql_Insert) == -1)
        return -1;
    return 0;
}
