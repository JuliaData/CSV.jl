// reads a corpus (one value per line), parses every line with fast_float N times,
// prints ns/value and an xor checksum of the bit patterns (for cross-checking)
#include "fast_float.h"
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>
int main(int argc, char** argv) {
    std::ifstream in(argv[1]);
    std::string line; std::vector<std::string> lines;
    while (std::getline(in, line)) lines.push_back(line);
    int reps = 7; double best = 1e18; uint64_t chk = 0;
    for (int r = 0; r < reps; ++r) {
        chk = 0;
        auto t0 = std::chrono::steady_clock::now();
        for (auto& s : lines) {
            double v = 0.0;
            auto res = fast_float::from_chars(s.data(), s.data() + s.size(), v);
            uint64_t bits; std::memcpy(&bits, &v, 8);
            if (res.ec != std::errc() || res.ptr != s.data() + s.size()) bits = 0xdeadbeef;
            chk ^= bits + 0x9e3779b97f4a7c15ULL * (uint64_t)s.size();
        }
        auto t1 = std::chrono::steady_clock::now();
        double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
        if (ns < best) best = ns;
    }
    std::printf("%.2f %llx\n", best / lines.size(), (unsigned long long)chk);
    return 0;
}
