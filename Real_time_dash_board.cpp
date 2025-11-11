#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <string>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <atomic>

using namespace std;

struct LogEntry {
    string level;
    double latency; // in ms
    chrono::system_clock::time_point timestamp;
};

// Shared resources
queue<LogEntry> logQueue;
mutex mtx;
condition_variable cv;
bool stopProcessing = false;

// Global statistics
struct Stats {
    atomic<int> totalLogs;
    atomic<int> errorCount;
    atomic<double> totalLatency;
    chrono::time_point<chrono::system_clock> startTime;

    Stats() {
        totalLogs = 0;
        errorCount = 0;
        totalLatency = 0.0;
        startTime = chrono::system_clock::now();
    }
} stats;

// Utility: Random log generator
string getRandomLogLevel() {
    int r = rand() % 100;
    if (r < 70) return "INFO";
    else if (r < 90) return "WARN";
    else return "ERROR";
}

// Producer: generates logs and pushes to queue
void logProducer(int producerId) {
    while (!stopProcessing) {
        this_thread::sleep_for(chrono::milliseconds(50 + rand() % 100));

        LogEntry log;
        log.level = getRandomLogLevel();
        log.latency = 10 + rand() % 100;
        log.timestamp = chrono::system_clock::now();

        unique_lock<mutex> lock(mtx);
        logQueue.push(log);
        cv.notify_one();
    }
}

// Consumer: processes logs and updates stats
void logConsumer() {
    while (true) {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [] { return !logQueue.empty() || stopProcessing; });

        if (stopProcessing && logQueue.empty())
            break;

        LogEntry log = logQueue.front();
        logQueue.pop();
        lock.unlock();

        stats.totalLogs++;
        stats.totalLatency += log.latency;
        if (log.level == "ERROR")
            stats.errorCount++;
    }
}

// Dashboard: displays metrics every 2 seconds
void dashboard() {
    while (!stopProcessing) {
        this_thread::sleep_for(chrono::seconds(2));

        int total = stats.totalLogs.load();
        int errors = stats.errorCount.load();
        double totalLat = stats.totalLatency.load();

        double elapsed = chrono::duration<double>(chrono::system_clock::now() - stats.startTime).count();
        double logsPerSec = (elapsed > 0) ? total / elapsed : 0;
        double avgLatency = (total > 0) ? totalLat / total : 0;
        double errorPercent = (total > 0) ? (100.0 * errors / total) : 0;

        // Clear screen (works in most terminals)
        system("cls"); // use "clear" if on Linux/Mac

        cout << "=================== REAL-TIME LOG ANALYTICS DASHBOARD ===================\n";
        cout << fixed << setprecision(2);
        cout << "Total Logs Processed : " << total << endl;
        cout << "Error Percentage     : " << errorPercent << " %" << endl;
        cout << "Average Latency      : " << avgLatency << " ms" << endl;
        cout << "Throughput           : " << logsPerSec << " logs/sec" << endl;
        cout << "=========================================================================\n";
    }
}

int main() {
    srand(time(0));

    cout << "Starting Real-Time Log Analytics System..." << endl;

    // Start threads
    thread producer1(logProducer, 1);
    thread producer2(logProducer, 2);
    thread consumer(logConsumer);
    thread ui(dashboard);

    // Run for 15 seconds
    this_thread::sleep_for(chrono::seconds(15));
    stopProcessing = true;
    cv.notify_all();

    producer1.join();
    producer2.join();
    consumer.join();
    ui.join();

    cout << "\nSystem stopped. Final statistics displayed above.\n";
    return 0;
}