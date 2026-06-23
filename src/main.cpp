#include <arpa/inet.h>
#include <cstdlib>
#include <libpq-fe.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include "mpmc_queue.h"

using json = nlohmann::json;

// -----------------
// Shared MPMC Queue
// -----------------
constexpr size_t QUEUE_CAPACITY = 16384;
MPMCQueue<json, QUEUE_CAPACITY> tradeQueue;

// --------------------------
// In-memory issuer info map
// --------------------------
struct IssuerInfo {
    std::string rating;
    std::string industry;
};

std::unordered_map<std::string, IssuerInfo> issuerMap;

// --------------------------------
// Load issuer_info from PostgreSQL
// --------------------------------
bool loadIssuerInfo(const std::string& conninfo) {
    PGconn* conn = PQconnectdb(conninfo.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << "\n";
        PQfinish(conn);
        return false;
    }

    PGresult* res = PQexec(conn, "SELECT issuer, rating, industry FROM issuer_info;");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "SELECT failed: " << PQerrorMessage(conn) << "\n";
        PQclear(res);
        PQfinish(conn);
        return false;
    }

    int rows = PQntuples(res);
    for (int i = 0; i < rows; ++i) {
        std::string issuer = PQgetvalue(res, i, 0);
        std::string rating = PQgetvalue(res, i, 1);
        std::string industry = PQgetvalue(res, i, 2);
        issuerMap[issuer] = {rating, industry};
    }

    PQclear(res);
    PQfinish(conn);
    std::cout << "Loaded " << issuerMap.size() << " issuers into memory\n";

    return true;
}

// ---------------------------------------
// Insert trade into PostgreSQL hypertable
// ---------------------------------------
bool insertTrade(PGconn* conn, const json& msg) {
    if (!conn) return false;

    const char* sql =
        "INSERT INTO trades (control_id, coupon, cusip, dealer_id, exec_time, "
        "industry, issuer, maturity, modifier3, price, rating, report_time, "
        "reporting_capacity, side, volume) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15);";

    std::vector<std::string> paramsStr(15);
    paramsStr[0]  = msg.value("control_id", "");
    paramsStr[1]  = msg.contains("coupon") ? std::to_string(msg["coupon"].get<double>()) : "";
    paramsStr[2]  = msg.value("cusip", "");
    paramsStr[3]  = msg.contains("dealer_id") ? std::to_string(msg["dealer_id"].get<int>()) : "";
    paramsStr[4]  = msg.value("exec_time", "");
    paramsStr[5]  = msg.value("industry", "");
    paramsStr[6]  = msg.value("issuer", "");
    paramsStr[7]  = msg.value("maturity", "");
    paramsStr[8]  = msg.value("modifier3", "");
    paramsStr[9]  = msg.contains("price") ? std::to_string(msg["price"].get<double>()) : "";
    paramsStr[10] = msg.value("rating", "");
    paramsStr[11] = msg.value("report_time", "");
    paramsStr[12] = msg.value("reporting_capacity", "");
    paramsStr[13] = msg.value("side", "");
    paramsStr[14] = msg.contains("volume") ? std::to_string(msg["volume"].get<long long>()) : "";

    const char* paramValues[15];
    for (size_t i = 0; i < 15; ++i) {
        paramValues[i] = paramsStr[i].c_str();
    }

    PGresult* res = PQexecParams(conn, sql, 15, nullptr, paramValues, nullptr, nullptr, 0);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "Insert failed: " << PQerrorMessage(conn) << "\n";
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}

// ----------------------------
// TCP Reader Thread (Producer)
// ----------------------------
void tcpReader(const std::string& host, int port, int producerId) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        return;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr);

    if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        // Retry with backoff — the feed generator may not be listening yet
        close(sock);
        for (int attempt = 1; attempt <= 10; ++attempt) {
            std::cerr << "[Producer " << producerId << "] Connect to port " << port
                      << " failed, retry " << attempt << "/10...\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));

            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                perror("Socket creation failed");
                return;
            }
            if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) == 0) {
                break;  // connected
            }
            close(sock);
            if (attempt == 10) {
                std::cerr << "[Producer " << producerId << "] Failed to connect after 10 attempts\n";
                return;
            }
        }
    }

    std::cout << "[Producer " << producerId << "] Connected to TRACE feed on port " << port << "\n";

    std::string buffer;
    char readBuf[1024];
    while (true) {
        ssize_t n = read(sock, readBuf, sizeof(readBuf));
        if (n <= 0) break;

        buffer.append(readBuf, n);

        size_t pos;
        while ((pos = buffer.find('\n')) != std::string::npos) {
            std::string line = buffer.substr(0, pos);
            buffer.erase(0, pos + 1);

            try {
                auto msg = json::parse(line);

                // Enrich with issuer info
                if (msg.contains("issuer")) {
                    auto it = issuerMap.find(msg["issuer"].get<std::string>());
                    if (it != issuerMap.end()) {
                        msg["rating"] = it->second.rating;
                        msg["industry"] = it->second.industry;
                    }
                }

                // Enqueue into MPMC queue
                while (!tradeQueue.enqueue(msg)) {
                    std::this_thread::sleep_for(std::chrono::microseconds(50));
                }

            } 
            catch (json::parse_error& e) {
                std::cerr << "[Producer " << producerId << "] JSON parse error: " << e.what() << "\n";
            }
        }
    }

    close(sock);
    std::cout << "[Producer " << producerId << "] TCP connection closed\n";
}

// ---------------
// Consumer Thread
// ---------------
void consumer(int consumerId, const char* conninfo) {
    PGconn* dbConn = PQconnectdb(conninfo);
    if (PQstatus(dbConn) != CONNECTION_OK) {
        std::cerr << "[Consumer " << consumerId << "] DB connection failed: " << PQerrorMessage(dbConn) << "\n";
        PQfinish(dbConn);
        return;
    }

    while (true) {
        json msg;
        while (!tradeQueue.dequeue(msg)) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }

        std::cout << "[Consumer " << consumerId << "] Got trade: " << msg.dump() << "\n";

        // Insert into DB
        if (!insertTrade(dbConn, msg)) {
            std::cerr << "[Consumer " << consumerId << "] Failed to insert trade\n";
        }
    }

    PQfinish(dbConn);
}

// -------------
// Main Function
// -------------
int main() {
    // Configuration from environment (for Docker) with local defaults
    const char* env_host = std::getenv("TRACE_HOST");
    const char* env_conninfo = std::getenv("PG_CONNINFO");

    const std::string host = env_host ? env_host : "127.0.0.1";
    const std::vector<int> ports = {5555, 5556, 5557};
    const int numConsumers = 2;
    const char* conninfo = env_conninfo
        ? env_conninfo
        : "dbname=finance user=douglas host=/var/run/postgresql";

    // Load issuer info
    if (!loadIssuerInfo(conninfo)) {
        std::cerr << "Failed to load issuer info. Exiting.\n";
        return 1;
    }

    // Launch producers
    std::vector<std::thread> producers;
    for (size_t i = 0; i < ports.size(); ++i) {
        producers.emplace_back(tcpReader, host, ports[i], static_cast<int>(i + 1));
    }

    // Launch consumers
    std::vector<std::thread> consumers;
    for (int i = 0; i < numConsumers; ++i) {
        consumers.emplace_back(consumer, i + 1, conninfo);
    }

    // Join threads
    for (auto& t : producers) t.join();
    for (auto& t : consumers) t.join();

    return 0;
}

