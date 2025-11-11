#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <sstream>
#include <thread>
#include <vector>
#include <stdexcept>

// ---------------- Thread-safe queue ----------------
template<typename T>
class TSQueue {
    std::queue<T> q;
    mutable std::mutex m;
    std::condition_variable cv;
    bool closed = false;
public:
    void push(T item) {
        {
            std::lock_guard<std::mutex> lk(m);
            if (closed) throw std::runtime_error("push on closed queue");
            q.push(std::move(item));
        }
        cv.notify_one();
    }

    // pop waits until available or closed + empty; returns false if queue closed and empty
    bool pop(T &out) {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&]{ return !q.empty() || closed; });
        if (q.empty()) return false;
        out = std::move(q.front()); q.pop();
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lk(m);
            closed = true;
        }
        cv.notify_all();
    }
};

// ---------------- Utilities ----------------
std::string make_log_line(int id, const std::string &event_type) {
    std::ostringstream oss;
    oss << "{ \"id\": " << id << ", \"ts\": "
        << std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch()).count()
        << ", \"event\": \"" << event_type << "\" }";
    return oss.str();
}

std::string random_event(std::mt19937 &rng) {
    static const std::vector<std::string> events = {"click", "view", "purchase", "error", "signup"};
    std::uniform_int_distribution<int> d(0, static_cast<int>(events.size()) - 1);
    return events[d(rng)];
}

// naive parser: extracts event value from string
std::string parse_event(const std::string &log) {
    auto pos = log.find("\"event\":");
    if (pos == std::string::npos) return "unknown";
    auto start = log.find('"', pos + 8);
    if (start == std::string::npos) return "unknown";
    start++;
    auto end = log.find('"', start);
    if (end == std::string::npos) return "unknown";
    return log.substr(start, end - start);
}

// ---------------- Aggregator ----------------
class Aggregator {
    std::mutex m;
    // metrics: event_type -> count
    std::map<std::string, uint64_t> counts;
public:
    void add_batch(const std::vector<std::string> &events) {
        std::lock_guard<std::mutex> lk(m);
        for (const auto &e : events) counts[e]++;
    }

    std::map<std::string, uint64_t> snapshot_and_clear() {
        std::lock_guard<std::mutex> lk(m);
        auto copy = counts;
        counts.clear();
        return copy;
    }

    std::map<std::string, uint64_t> snapshot_no_clear() {
        std::lock_guard<std::mutex> lk(m);
        return counts;
    }
};

// ---------------- Producer ----------------
void producer_fn(TSQueue<std::string> &q, int producer_id, std::atomic<bool> &running) {
    std::mt19937 rng(static_cast<unsigned>(
        std::chrono::high_resolution_clock::now().time_since_epoch().count() + producer_id));
    int id = 0;
    while (running.load()) {
        std::string ev = random_event(rng);
        auto log = make_log_line(id++, ev);
        q.push(log);
        // variable produce rate
        std::this_thread::sleep_for(std::chrono::milliseconds(5 + (rng() % 20)));
    }
}

// ---------------- Consumer (with batching) ----------------
void consumer_fn(TSQueue<std::string> &q, Aggregator &agg,
                 size_t batch_size, std::chrono::milliseconds batch_timeout,
                 std::atomic<bool> &running, int /*consumer_id*/)
{
    std::vector<std::string> batch;
    batch.reserve(batch_size);
    auto last_flush = std::chrono::steady_clock::now();

    while (true) {
        std::string log;
        bool got = q.pop(log);
        if (!got) {
            // queue closed and empty
            if (!batch.empty()) {
                std::vector<std::string> events;
                for (auto &l : batch) events.push_back(parse_event(l));
                agg.add_batch(events);
                batch.clear();
            }
            break;
        }

        // transform
        auto event = parse_event(log);
        batch.push_back(event);

        auto now = std::chrono::steady_clock::now();
        bool timeout_expired = (now - last_flush) >= batch_timeout;
        if (batch.size() >= batch_size || timeout_expired) {
            // process batch
            agg.add_batch(batch);
            batch.clear();
            last_flush = now;
        }
    }
}

// ---------------- Main ----------------
int main() {
    TSQueue<std::string> tsqueue;
    Aggregator aggregator;

    std::atomic<bool> running{true};

    // Start producers
    const int NUM_PRODUCERS = 3;
    std::vector<std::thread> producers;
    producers.reserve(NUM_PRODUCERS);
    for (int i = 0; i < NUM_PRODUCERS; ++i)
        producers.emplace_back(producer_fn, std::ref(tsqueue), i, std::ref(running));

    // Start consumers
    const int NUM_CONSUMERS = 4;
    std::vector<std::thread> consumers;
    consumers.reserve(NUM_CONSUMERS);
    size_t batch_size = 100; // micro-batch size
    auto batch_timeout = std::chrono::milliseconds(2000); // flush every 2s if not enough items
    for (int i = 0; i < NUM_CONSUMERS; ++i)
        consumers.emplace_back(consumer_fn, std::ref(tsqueue), std::ref(aggregator),
                               batch_size, batch_timeout, std::ref(running), i);

    // Monitoring thread: print aggregates every 5 seconds (snapshot & clear)
    std::thread monitor([&](){
        for (int i = 0; i < 12; ++i) { // run for 12 intervals then stop
            std::this_thread::sleep_for(std::chrono::seconds(5));
            auto snap = aggregator.snapshot_and_clear();
            std::cout << "=== Aggregated metrics (5s window) ===\n";
            for (auto &p : snap) {
                std::cout << p.first << " -> " << p.second << "\n";
            }
            std::cout << "=====================================\n";
        }
        // stop producers after monitoring rounds
        running.store(false);
        tsqueue.close();
    });

    // join producers
    for (auto &t : producers) if (t.joinable()) t.join();
    // join consumers
    for (auto &t : consumers) if (t.joinable()) t.join();
    if (monitor.joinable()) monitor.join();

    std::cout << "Shutting down. Final totals (non-cleared snapshot):\n";
    auto final = aggregator.snapshot_no_clear();
    for (auto &p : final) std::cout << p.first << " -> " << p.second << "\n";

    return 0;
}
